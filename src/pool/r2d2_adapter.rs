use r2d2::ManageConnection;
use r2d2_sqlite::SqliteConnectionManager;

use crate::{embedded_milli::Instance, TableIndexSettings};

use super::SqliteConnectionHandler;

pub struct SkaldConnectionManager {
    inner: SqliteConnectionManager,
    handler: SqliteConnectionHandler,
}

impl SkaldConnectionManager {
    pub fn new(inner: SqliteConnectionManager, instance: Instance) -> Self {
        Self {
            handler: SqliteConnectionHandler::new(inner.connect().unwrap(), instance),
            inner,
        }
    }

    pub fn with_table(self, table: String, settings: Vec<TableIndexSettings>) -> Self {
        Self {
            handler: self.handler.with_table(table, settings),
            inner: self.inner,
        }
    }
}

impl ManageConnection for SkaldConnectionManager {
    type Connection = rusqlite::Connection;

    type Error = rusqlite::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let connection = self.inner.connect()?;
        self.handler.attach_hooks(&connection);
        Ok(connection)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        self.inner.is_valid(conn)
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        self.inner.has_broken(conn)
    }
}
