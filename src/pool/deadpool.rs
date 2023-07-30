use async_trait::async_trait;
use deadpool::{managed, Runtime};
use deadpool_sqlite::{Config, ConfigError, Metrics};
use deadpool_sync::SyncWrapper;

use crate::{embedded_milli::Instance, TableIndexSettings};

use super::SqliteConnectionHandler;

deadpool::managed_reexports!(
    "skald",
    Manager,
    deadpool::managed::Object<Manager>,
    rusqlite::Error,
    ConfigError
);

pub struct Manager {
    inner: deadpool_sqlite::Manager,
    handler: SqliteConnectionHandler,
}

impl Manager {
    #[must_use]
    pub fn from_config(config: &Config, runtime: Runtime, instance: Instance) -> Self {
        let path = config.path.clone();
        let conn = rusqlite::Connection::open(path).unwrap();
        let inner = deadpool_sqlite::Manager::from_config(config, runtime);

        Self {
            handler: SqliteConnectionHandler::new(conn, instance),
            inner,
        }
    }

    pub fn with_table(
        self,
        database: String,
        table: String,
        settings: Vec<TableIndexSettings>,
    ) -> Self {
        Self {
            handler: self.handler.with_table(database, table, settings),
            inner: self.inner,
        }
    }
}

#[async_trait]
impl managed::Manager for Manager {
    type Type = SyncWrapper<rusqlite::Connection>;
    type Error = rusqlite::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let conn = self.inner.create().await?;
        self.handler.attach_hooks(&conn.lock().unwrap());
        Ok(conn)
    }

    async fn recycle(
        &self,
        conn: &mut Self::Type,
        metrics: &Metrics,
    ) -> managed::RecycleResult<Self::Error> {
        self.inner.recycle(conn, metrics).await
    }
}
