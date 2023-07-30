use crate::{embedded_milli::Instance, DashMapExt, StatementExt, TableIndexSettings, TableUpdate};
use crossbeam::{channel, select};
use dashmap::DashMap;
use parking_lot::RwLock;
use rusqlite::{hooks::Action, preupdate_hook::PreUpdateCase, Connection};
use std::{
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};

#[cfg(feature = "deadpool")]
pub mod deadpool;
#[cfg(feature = "r2d2")]
pub mod r2d2;
#[cfg(feature = "sqlx")]
pub mod sqlx;

pub struct SqliteConnectionHandler {
    table_settings: Arc<DashMap<(String, String), Vec<TableIndexSettings>>>,
    update_tx: channel::Sender<DashMap<String, Vec<TableUpdate>>>,
    _updater_handle: JoinHandle<()>,
}

impl SqliteConnectionHandler {
    pub fn new(conn: Connection, instance: Instance) -> Self {
        let (update_tx, update_rx) = channel::unbounded();

        let handle = thread::spawn(move || index_updater(instance, update_rx, conn));
        Self {
            table_settings: Default::default(),
            update_tx,
            _updater_handle: handle,
        }
    }

    pub fn with_table(
        self,
        database: String,
        table: String,
        settings: Vec<TableIndexSettings>,
    ) -> Self {
        {
            let mut index_updates = self.table_settings.get_or_insert_entry((database, table));
            index_updates.get_mut().extend(settings);
        }

        self
    }

    pub fn attach_hooks(&self, connection: &Connection) {
        let table_settings = self.table_settings.clone();
        let pending_updates = Arc::new(RwLock::new(DashMap::<_, Vec<TableUpdate>>::new()));
        let pending_updates_ = pending_updates.clone();
        connection.preupdate_hook(Some(
            move |_action, db_name: &str, table_name: &str, preupdate_case: &_| {
                if let PreUpdateCase::Delete(accessor) = preupdate_case {
                    let index_settings = table_settings
                        .get(&(db_name.to_owned(), table_name.to_owned()))
                        .unwrap();
                    for settings in index_settings.iter() {
                        let primary_key = (settings.primary_key_fn.0)(accessor);
                        let pending_updates_read = pending_updates_.read();
                        let mut entry =
                            pending_updates_read.get_or_insert_entry(settings.index_name.clone());

                        entry.get_mut().push(TableUpdate::Delete { primary_key });
                    }
                }
            },
        ));

        let table_settings = self.table_settings.clone();
        let pending_updates_ = pending_updates.clone();
        connection.update_hook(Some(
            move |action, db_name: &str, table_name: &str, rowid| {
                if let Action::SQLITE_INSERT | Action::SQLITE_UPDATE = action {
                    let index_settings = table_settings
                        .get(&(db_name.to_owned(), table_name.to_owned()))
                        .unwrap();
                    for settings in index_settings.iter() {
                        let pending_updates_read = pending_updates_.read();
                        let mut entry =
                            pending_updates_read.get_or_insert_entry(settings.index_name.clone());
                        entry.get_mut().push(TableUpdate::Upsert {
                            rowid,
                            update_query: settings.update_query.clone(),
                        });
                    }
                }
            },
        ));

        let pending_updates_ = pending_updates.clone();
        let update_tx = self.update_tx.clone();
        connection.commit_hook(Some(move || {
            let old = std::mem::take(&mut *pending_updates_.write());
            update_tx.send(old).unwrap();
            false
        }));

        connection.rollback_hook(Some(move || {
            pending_updates.read().clear();
        }));
    }
}

fn index_updater(
    instance: Instance,
    update_rx: channel::Receiver<DashMap<String, Vec<TableUpdate>>>,
    connection: Connection,
) {
    loop {
        let updates = update_rx.recv().unwrap();

        loop {
            select! {
                recv(update_rx) -> msg => {
                    let msg = msg.unwrap();
                    for (key, val) in msg.into_iter() {
                        let mut index_updates = updates.get_or_insert_entry(key);
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
                let docs = statement.query_to_json([rowid]);

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
