use std::{
    collections::HashMap,
    fs,
    io::Cursor,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{anyhow, Result};
use derivative::Derivative;
use milli::{
    documents::{DocumentsBatchBuilder, DocumentsBatchReader},
    heed, update, Criterion, Index, Search, SearchResult,
};
use once_cell::sync::Lazy;
use parking_lot::RwLock;

// The following constants are for the map size used in heed/LMDB.
// We assume any OS we run on will have a page size less than 16 MiB (2^24)
// and that 16 MiB will be a multiple of the OS page size (which it should be).
// MAX_POSSIBLE_SIZE complies with memory constraints imposed by iOS without extra entitlements.
const MAX_POSSIBLE_SIZE: usize = 2_000_000_000;
const MAX_OS_PAGE_SIZE: usize = 16_777_216;

// These are needed because of iOS nonsense; see: https://github.com/GregoryConrad/mimir/issues/227
const MAP_EXP_BACKOFF_AMOUNT: f32 = 0.85;
const MAP_SIZE_TRIES: i32 = 8;

/// Represents a document in a milli index
pub type Document = serde_json::Map<String, serde_json::Value>;

#[derive(Clone)]
pub struct EmbeddedMilli {
    index: Index,
}

/// Represents the synonyms of a given word
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Synonyms {
    pub word: String,
    pub synonyms: Vec<String>,
}

#[derive(Clone, Eq, PartialEq, Derivative)]
#[derivative(Debug, Default)]
pub struct IndexSettings {
    pub primary_key: Option<String>,
    pub searchable_fields: Option<Vec<String>>,
    pub filterable_fields: Vec<String>,
    pub sortable_fields: Vec<String>,
    pub ranking_rules: Vec<String>,
    pub stop_words: Vec<String>,
    pub synonyms: Vec<Synonyms>,
    #[derivative(Default(value = "true"))]
    pub typos_enabled: bool,
    pub min_word_size_for_one_typo: Option<u8>,
    pub min_word_size_for_two_typos: Option<u8>,
    pub disallow_typos_on_words: Vec<String>,
    pub disallow_typos_on_fields: Vec<String>,
}

const CURRENT_MILLI_VERSION: u32 = 1;

static INDEXES: Lazy<RwLock<HashMap<PathBuf, EmbeddedMilli>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

#[derive(Clone)]
pub struct Instance {
    instance_dir: PathBuf,
}

impl Instance {
    pub fn new(instance_dir: impl Into<PathBuf>) -> Self {
        Self {
            instance_dir: instance_dir.into(),
        }
    }

    pub fn get_milli_version(&self) -> u32 {
        let contents =
            fs::read_to_string(Path::new(&self.instance_dir).join("milli_version")).unwrap();
        contents.parse().unwrap()
    }

    pub fn needs_rebuilding(&self) -> bool {
        self.get_milli_version() < CURRENT_MILLI_VERSION
    }

    pub fn get_index(&self, name: impl AsRef<str>) -> Result<EmbeddedMilli> {
        let dir = self.instance_dir.join(name.as_ref());
        if let Some(index) = INDEXES.read().get(&dir) {
            return Ok(index.clone());
        }
        std::fs::create_dir_all(&dir)?;

        // We need this exponential backoff retry crap due to iOS' limited address space,
        // *despite iOS being 64 bit*. See https://github.com/GregoryConrad/mimir/issues/227
        let mut map_size;
        let mut retry = 0;
        loop {
            // Find the maximum multiple of MAX_OS_PAGE_SIZE that is less than curr_max_map_size.
            let curr_max_map_size =
                (MAX_POSSIBLE_SIZE as f32 * MAP_EXP_BACKOFF_AMOUNT.powi(retry)) as usize;
            map_size = curr_max_map_size - (curr_max_map_size % MAX_OS_PAGE_SIZE);
            let env_result = heed::EnvOpenOptions::new().map_size(map_size).open(&dir);
            match env_result {
                Ok(env) => {
                    env.prepare_for_closing();
                    break;
                }
                Err(_) if retry < MAP_SIZE_TRIES => {
                    retry += 1;
                    continue;
                }
                err @ Err(_) => {
                    err?;
                }
            };
        }

        let mut options = heed::EnvOpenOptions::new();
        options.map_size(map_size);

        let index = Index::new(options, &dir).map_err(anyhow::Error::from)?;
        let res = EmbeddedMilli { index };
        INDEXES.write().insert(dir, res.clone());
        Ok(res)
    }
}

impl EmbeddedMilli {
    pub fn write(&self) -> heed::RwTxn<'_, '_> {
        self.index.write_txn().unwrap()
    }

    pub fn read(&self) -> heed::RoTxn<'_> {
        self.index.read_txn().unwrap()
    }

    pub fn add_documents<'t>(
        &'t self,
        wtxn: &mut heed::RwTxn<'t, '_>,
        documents: Vec<Document>,
    ) -> Result<()> {
        // Create a batch builder to convert json_documents into milli's format
        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        for doc in documents {
            builder.append_json_object(&doc)?;
        }

        // Flush the contents of the builder and retreive the buffer to make a batch reader
        let buff = builder.into_inner()?;
        let reader = DocumentsBatchReader::from_reader(Cursor::new(buff))?;

        // Create the configs needed for the batch document addition
        let indexer_config = update::IndexerConfig::default();
        let indexing_config = update::IndexDocumentsConfig::default();

        // Make an index write transaction with a batch step to index the new documents

        let (builder, indexing_result) = update::IndexDocuments::new(
            wtxn,
            &self.index,
            &indexer_config,
            indexing_config,
            |_| (),
            || false,
        )?
        .add_documents(reader)?;
        indexing_result?; // check to make sure there is no UserError
        builder.execute()?;

        Ok(())
    }

    pub fn delete_documents<'t>(
        &'t self,
        wtxn: &mut heed::RwTxn<'t, '_>,
        document_ids: Vec<String>,
    ) -> Result<()> {
        let mut builder = update::DeleteDocuments::new(wtxn, &self.index)?;
        for doc_id in document_ids {
            builder.delete_external_id(&doc_id);
        }
        builder.execute()?;

        Ok(())
    }

    pub fn delete_all_documents<'t>(&'t self, wtxn: &mut heed::RwTxn<'t, '_>) -> Result<()> {
        let builder = update::ClearDocuments::new(wtxn, &self.index);
        builder.execute()?;

        Ok(())
    }

    pub fn set_documents<'t>(
        &'t self,
        wtxn: &mut heed::RwTxn<'t, '_>,
        documents: Vec<Document>,
    ) -> Result<()> {
        // Delete all existing documents
        update::ClearDocuments::new(wtxn, &self.index).execute()?;

        // Create a batch builder to convert json_documents into milli's format
        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        for doc in documents {
            builder.append_json_object(&doc)?;
        }

        // Flush the contents of the builder and retreive the buffer to make a batch reader
        let buff = builder.into_inner()?;
        let reader = DocumentsBatchReader::from_reader(Cursor::new(buff))?;

        // Create the configs needed for the batch document addition
        let indexer_config = update::IndexerConfig::default();
        let indexing_config = update::IndexDocumentsConfig::default();

        // Make a batch step to index the new documents
        let (builder, indexing_result) = update::IndexDocuments::new(
            wtxn,
            &self.index,
            &indexer_config,
            indexing_config,
            |_| (),
            || false,
        )?
        .add_documents(reader)?;
        indexing_result?; // check to make sure there is no UserError
        builder.execute()?;

        Ok(())
    }

    pub fn get_document(
        &self,
        rtxn: &heed::RoTxn,
        document_id: String,
    ) -> Result<Option<Document>> {
        let fields_ids_map = self.index.fields_ids_map(rtxn)?;
        let external_ids = self.index.external_documents_ids(rtxn)?;
        let internal_id = match external_ids.get(document_id) {
            Some(id) => id,
            None => return Ok(None),
        };
        let documents = self.index.documents(rtxn, vec![internal_id])?;
        let (_id, document) = documents
            .first()
            .ok_or_else(|| anyhow!("Missing document"))?;
        milli::all_obkv_to_json(*document, &fields_ids_map)
            .map(Some)
            .map_err(anyhow::Error::from)
    }

    pub fn get_all_documents(&self) -> Result<Vec<Document>> {
        let rtxn = self.index.read_txn()?;
        let fields_ids_map = self.index.fields_ids_map(&rtxn)?;
        let documents = self.index.all_documents(&rtxn)?;
        documents
            .map(|doc| milli::all_obkv_to_json(doc?.1, &fields_ids_map))
            .map(|r| r.map_err(anyhow::Error::from))
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn search_documents(
        &self,
        rtxn: &heed::RoTxn,
        build_search: impl FnOnce(&mut Search),
    ) -> Result<Vec<Document>> {
        // Create the search

        let mut search = Search::new(rtxn, &self.index);
        build_search(&mut search);

        // Get the documents based on the search results
        let SearchResult { documents_ids, .. } = search.execute()?;
        let fields_ids_map = self.index.fields_ids_map(rtxn)?;
        self.index
            .documents(rtxn, documents_ids)?
            .iter()
            .map(|(_id, doc)| milli::all_obkv_to_json(*doc, &fields_ids_map))
            .map(|r| r.map_err(anyhow::Error::from))
            .collect()
    }

    pub fn number_of_documents(&self, rtxn: &heed::RoTxn) -> Result<u64> {
        self.index
            .number_of_documents(rtxn)
            .map_err(anyhow::Error::from)
    }

    pub fn get_settings(&self, rtxn: &heed::RoTxn) -> Result<IndexSettings> {
        Ok(IndexSettings {
            primary_key: self.index.primary_key(rtxn)?.map(Into::into),
            searchable_fields: self
                .index
                .searchable_fields(rtxn)?
                .map(|fields| fields.into_iter().map(String::from).collect()),
            filterable_fields: self.index.filterable_fields(rtxn)?.into_iter().collect(),
            sortable_fields: self.index.sortable_fields(rtxn)?.into_iter().collect(),
            ranking_rules: self
                .index
                .criteria(rtxn)?
                .into_iter()
                .map(|rule| match rule {
                    Criterion::Words => "words".to_owned(),
                    Criterion::Typo => "typo".to_owned(),
                    Criterion::Proximity => "proximity".to_owned(),
                    Criterion::Attribute => "attribute".to_owned(),
                    Criterion::Sort => "sort".to_owned(),
                    Criterion::Exactness => "exactness".to_owned(),
                    Criterion::Asc(val) => format!("{val}:asc"),
                    Criterion::Desc(val) => format!("{val}:desc"),
                })
                .collect(),
            stop_words: self
                .index
                .stop_words(rtxn)?
                .map(|words| words.stream().into_strs())
                .unwrap_or_else(|| Ok(Vec::new()))?,
            synonyms: self
                .index
                .synonyms(rtxn)?
                .into_iter()
                .map(|(word, synonyms)| {
                    (
                        word[0].clone(),
                        synonyms
                            .iter()
                            .flat_map(|v| v.iter())
                            .map(String::from)
                            .collect(),
                    )
                })
                .map(|(word, synonyms)| Synonyms { word, synonyms })
                .collect(),
            typos_enabled: self.index.authorize_typos(rtxn)?,
            min_word_size_for_one_typo: Some(self.index.min_word_len_one_typo(rtxn)?),
            min_word_size_for_two_typos: Some(self.index.min_word_len_two_typos(rtxn)?),
            disallow_typos_on_words: self
                .index
                .exact_words(rtxn)?
                .map(|words| words.stream().into_strs())
                .unwrap_or_else(|| Ok(Vec::new()))?,
            disallow_typos_on_fields: self
                .index
                .exact_attributes(rtxn)?
                .into_iter()
                .map(String::from)
                .collect(),
        })
    }

    pub fn set_settings<'t>(
        &'t self,
        wtxn: &mut heed::RwTxn<'t, '_>,
        settings: IndexSettings,
    ) -> Result<()> {
        // Destructure the settings into the corresponding fields
        let IndexSettings {
            primary_key,
            searchable_fields,
            filterable_fields,
            sortable_fields,
            ranking_rules,
            stop_words,
            synonyms,
            typos_enabled,
            min_word_size_for_one_typo,
            min_word_size_for_two_typos,
            disallow_typos_on_words,
            disallow_typos_on_fields,
        } = settings;

        // Set up the settings update

        let indexer_config = update::IndexerConfig::default();
        let mut builder = update::Settings::new(wtxn, &self.index, &indexer_config);

        // Copy over the given settings
        match primary_key {
            Some(pkey) => builder.set_primary_key(pkey),
            None => builder.reset_primary_key(),
        }
        match searchable_fields {
            Some(fields) => builder.set_searchable_fields(fields),
            None => builder.reset_searchable_fields(),
        }
        builder.set_filterable_fields(filterable_fields.into_iter().collect());
        builder.set_sortable_fields(sortable_fields.into_iter().collect());
        builder.set_criteria(
            ranking_rules
                .iter()
                .map(String::as_str)
                .map(milli::Criterion::from_str)
                .map(|r| r.map_err(anyhow::Error::from))
                .collect::<Result<_>>()?,
        );
        builder.set_stop_words(stop_words.into_iter().collect());
        builder.set_synonyms(synonyms.into_iter().map(|s| (s.word, s.synonyms)).collect());
        builder.set_autorize_typos(typos_enabled);
        if let Some(val) = min_word_size_for_one_typo {
            builder.set_min_word_len_one_typo(val);
        }
        if let Some(val) = min_word_size_for_two_typos {
            builder.set_min_word_len_two_typos(val);
        }
        builder.set_exact_words(disallow_typos_on_words.into_iter().collect());
        builder.set_exact_attributes(disallow_typos_on_fields.into_iter().collect());

        // Execute the settings update
        builder.execute(|_| {}, || false)?;

        Ok(())
    }
}
