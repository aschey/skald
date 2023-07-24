use dashmap::{
    mapref::entry::{Entry, OccupiedEntry},
    DashMap,
};
use derivative::Derivative;
use embedded_milli::Document;
use rusqlite::{
    preupdate_hook::PreUpdateOldValueAccessor,
    types::{FromSql, ValueRef},
    Params, Statement,
};
use std::{convert::Infallible, hash::Hash, sync::Arc};

pub mod embedded_milli;
pub mod pool;

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

#[derive(Debug)]
pub enum TableUpdate {
    Delete { primary_key: String },
    Upsert { rowid: i64, update_query: String },
}

pub trait StatementExt {
    fn query_to_json<P: Params>(&mut self, params: P) -> Vec<Document>;
}

impl StatementExt for Statement<'_> {
    fn query_to_json<P: Params>(&mut self, params: P) -> Vec<Document> {
        self.query_map(params, |row| {
            let json_iter = (0..row.as_ref().column_count()).map(|col| {
                let column_name = row.as_ref().column_name(col).unwrap().to_owned();
                let sqlite_value_ref = row.get_ref_unwrap(col);

                let json_value = match sqlite_value_ref {
                    ValueRef::Text(sqlite_value) => {
                        if !sqlite_value.starts_with(&[b'"'])
                            && !sqlite_value.starts_with(&[b'{'])
                            && !sqlite_value.starts_with(&[b'['])
                        {
                            serde_json::Value::from(std::str::from_utf8(sqlite_value).unwrap())
                        } else {
                            serde_json::Value::column_result(sqlite_value_ref)
                                .or_else(|_| {
                                    Ok::<_, Infallible>(serde_json::Value::from(
                                        std::str::from_utf8(sqlite_value).unwrap(),
                                    ))
                                })
                                .unwrap()
                        }
                    }
                    _ => serde_json::Value::column_result(sqlite_value_ref).unwrap(),
                };

                (column_name, json_value)
            });
            Ok(serde_json::map::Map::from_iter(json_iter))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
    }
}

trait DashMapExt<K, V> {
    fn get_or_insert_entry(&self, key: K) -> OccupiedEntry<'_, K, V>;
}

impl<K, V> DashMapExt<K, V> for DashMap<K, V>
where
    K: PartialEq + Eq + Hash + Clone,
    V: Default,
{
    fn get_or_insert_entry(&self, key: K) -> OccupiedEntry<'_, K, V> {
        let entry = self.entry(key);
        match entry {
            Entry::Occupied(o) => o,
            Entry::Vacant(vacant) => vacant.insert_entry(V::default()),
        }
    }
}
