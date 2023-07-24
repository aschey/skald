use crossbeam::select;
use dashmap::{mapref::entry::Entry, DashMap};
use derivative::Derivative;
use embedded_milli::Instance;
use milli::{
    documents::{DocumentsBatchBuilder, DocumentsBatchReader},
    execute_search,
    heed::EnvOpenOptions,
    update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings},
    DefaultSearchLogger, GeoSortStrategy, Index, SearchContext, TermsMatchingStrategy,
};
use r2d2::ManageConnection;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{
    hooks::Action,
    preupdate_hook::{PreUpdateCase, PreUpdateOldValueAccessor},
    types::FromSql,
    Connection,
};
use serde_json::Number;
use std::{
    convert::Infallible,
    io::Cursor,
    sync::{Arc, Mutex, RwLock},
    thread,
    time::Duration,
};

pub mod embedded_milli;

#[derive(Clone)]
pub struct PrimaryKeyFn(Arc<dyn Fn(&PreUpdateOldValueAccessor) -> String + Send + Sync>);

impl PrimaryKeyFn {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&PreUpdateOldValueAccessor) -> String + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct TableIndexSettings {
    pub index_name: String,
    pub update_query: String,
    #[derivative(Debug = "ignore")]
    pub primary_key_fn: PrimaryKeyFn,
}

pub struct SqliteMilliConnectionManager {
    inner: SqliteConnectionManager,
    table_settings: Arc<DashMap<String, Vec<TableIndexSettings>>>,
    update_tx: crossbeam::channel::Sender<DashMap<String, Vec<TableUpdate>>>,
}

impl SqliteMilliConnectionManager {
    pub fn new(inner: SqliteConnectionManager, instance: Instance) -> Self {
        let (update_tx, update_rx) = crossbeam::channel::unbounded();
        let conn = inner.connect().unwrap();
        thread::spawn(move || index_updater(instance, update_rx, conn));
        Self {
            inner,
            table_settings: Default::default(),
            update_tx,
        }
    }

    pub fn with_table(self, table: String, settings: Vec<TableIndexSettings>) -> Self {
        {
            let mut index_updates = match self.table_settings.entry(table) {
                Entry::Occupied(o) => o,
                Entry::Vacant(vacant) => vacant.insert_entry(vec![]),
            };
            index_updates.get_mut().extend(settings);
        }

        self
    }
}

#[derive(Debug)]
pub enum TableUpdate {
    Delete { primary_key: String },
    Upsert { rowid: i64, update_query: String },
}

pub fn index_updater(
    instance: Instance,
    update_rx: crossbeam::channel::Receiver<DashMap<String, Vec<TableUpdate>>>,
    connection: Connection,
) {
    loop {
        let updates = update_rx.recv().unwrap();

        loop {
            select! {
                recv(update_rx) -> msg => {
                    let msg = msg.unwrap();
                    for (key, val) in msg.into_iter() {
                        let mut index_updates = match updates.entry(key) {
                            Entry::Occupied(o) => o,
                            Entry::Vacant(vacant) => vacant.insert_entry(vec![]),
                        };
                        index_updates.get_mut().extend(val);
                    }

                }
                default(Duration::from_millis(20)) => {
                    break;
                }
            }
        }

        for (index_name, updates) in updates.into_iter() {
            let index = instance.get_index(index_name).unwrap();
            let mut wtxn = index.write();

            let upserts = updates.iter().filter_map(|u| {
                if let TableUpdate::Upsert {
                    rowid,
                    update_query,
                } = u
                {
                    Some((rowid, update_query))
                } else {
                    None
                }
            });

            for (rowid, update_query) in upserts {
                let mut statement = connection.prepare_cached(update_query).unwrap();
                let docs: Vec<_> = statement
                    .query_map([rowid], |row| {
                        let mut row_map = serde_json::map::Map::new();
                        for col in 0..row.as_ref().column_count() {
                            let column_name = row.as_ref().column_name(col).unwrap().to_owned();
                            let sqlite_value_ref = row.get_ref_unwrap(col);

                            println!("VALUE REF {sqlite_value_ref:?}");
                            let json_value = match sqlite_value_ref {
                                rusqlite::types::ValueRef::Text(sqlite_value) => {
                                    if !sqlite_value.starts_with(&[b'"'])
                                        && !sqlite_value.starts_with(&[b'{'])
                                        && !sqlite_value.starts_with(&[b'['])
                                    {
                                        serde_json::Value::from(
                                            std::str::from_utf8(sqlite_value).unwrap(),
                                        )
                                    } else {
                                        serde_json::Value::column_result(sqlite_value_ref)
                                            .or_else(|e| {
                                                println!("JSON ERR {e:?}");
                                                Ok::<_, Infallible>(serde_json::Value::from(
                                                    std::str::from_utf8(sqlite_value).unwrap(),
                                                ))
                                            })
                                            .unwrap()
                                    }
                                }
                                _ => serde_json::Value::column_result(sqlite_value_ref).unwrap(),
                            };
                            println!("JSON VALUE {json_value:?}");
                            row_map.insert(column_name, json_value);
                        }
                        Ok(row_map)
                    })
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect();

                index.add_documents(&mut wtxn, docs).unwrap();
            }

            let keys_to_delete: Vec<_> = updates
                .iter()
                .filter_map(|u| {
                    if let TableUpdate::Delete { primary_key } = u {
                        Some(primary_key.to_owned())
                    } else {
                        None
                    }
                })
                .collect();

            index.delete_documents(&mut wtxn, keys_to_delete).unwrap();
            wtxn.commit().unwrap();
        }
    }
}

impl r2d2::ManageConnection for SqliteMilliConnectionManager {
    type Connection = rusqlite::Connection;

    type Error = rusqlite::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let connection = self.inner.connect()?;

        let table_settings = self.table_settings.clone();
        let pending_updates = Arc::new(RwLock::new(DashMap::new()));
        let pending_updates_ = pending_updates.clone();
        connection.preupdate_hook(Some(
            move |action, db_name: &_, table_name: &_, preupdate_case: &_| {
                println!("preupdate hook {action:?} {db_name} {table_name}, {preupdate_case:?}");
                if let PreUpdateCase::Delete(accessor) = preupdate_case {
                    let index_settings = table_settings.get(table_name).unwrap();
                    for settings in index_settings.iter() {
                        let primary_key = (settings.primary_key_fn.0)(accessor);
                        let pending_updates_read = pending_updates_.read().unwrap();
                        let entry = pending_updates_read.entry(settings.index_name.clone());
                        let mut index_updates = match entry {
                            Entry::Occupied(o) => o,
                            Entry::Vacant(vacant) => vacant.insert_entry(vec![]),
                        };
                        index_updates
                            .get_mut()
                            .push(TableUpdate::Delete { primary_key });
                    }
                }
            },
        ));

        let table_settings = self.table_settings.clone();
        let pending_updates_ = pending_updates.clone();
        connection.update_hook(Some(move |action, db_name: &_, table_name: &str, rowid| {
            println!("update hook {action:?} {db_name} {table_name}, {rowid}");
            if let Action::SQLITE_INSERT | Action::SQLITE_UPDATE = action {
                let index_settings = table_settings.get(table_name).unwrap();
                for settings in index_settings.iter() {
                    let pending_updates_read = pending_updates_.read().unwrap();
                    let entry = pending_updates_read.entry(settings.index_name.clone());
                    let mut index_updates = match entry {
                        Entry::Occupied(o) => o,
                        Entry::Vacant(vacant) => vacant.insert_entry(vec![]),
                    };
                    index_updates.get_mut().push(TableUpdate::Upsert {
                        rowid,
                        update_query: settings.update_query.clone(),
                    });
                }
            }
        }));
        let pending_updates_ = pending_updates.clone();
        let update_tx = self.update_tx.clone();
        connection.commit_hook(Some(move || {
            println!("COMMIT");
            let old = std::mem::take(&mut *pending_updates_.write().unwrap());
            update_tx.send(old).unwrap();
            false
        }));

        connection.rollback_hook(Some(move || {
            println!("ROLLBACK");
            pending_updates.read().unwrap().clear();
        }));
        Ok(connection)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        self.inner.is_valid(conn)
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        self.inner.has_broken(conn)
    }
}
