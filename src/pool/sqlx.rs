use super::SqliteConnectionHandler;
use crate::{
    embedded_milli::{Document, Instance},
    TableIndexSettings,
};
use async_trait::async_trait;
use futures_core::future::BoxFuture;
use sqlx::{
    pool::{PoolConnection, PoolConnectionMetadata},
    sqlite::SqliteRow,
    Column, Row, Sqlite, SqliteConnection, SqlitePool, TypeInfo, ValueRef,
};
use std::sync::{Arc, Mutex};

pub struct SkaldHooks {
    handler: SqliteConnectionHandler,
}

#[async_trait]
pub trait IntoConnection<C> {
    async fn into_connection(mut self) -> C;
}

#[async_trait]
pub trait AsConnection<C> {
    async fn as_connection(&mut self) -> C;
}

pub trait QueryExt {
    fn query_to_json(&self) -> Vec<Document>;
}

impl QueryExt for Vec<SqliteRow> {
    fn query_to_json(&self) -> Vec<Document> {
        self.iter()
            .map(|row| {
                let json_iter = row.columns().iter().map(|col| {
                    let raw = row.try_get_raw(col.name()).unwrap();
                    if raw.type_info().name() == "TEXT" {
                        let text = row.get_unchecked::<&str, _>(col.name());

                        if text.starts_with('[')
                            || text.starts_with('{')
                            || text.starts_with('"')
                            || text.starts_with('\'')
                        {
                            let val = row
                            .try_get_unchecked::<sqlx::types::Json<serde_json::value::Value>, _>(
                                col.name(),
                            );
                            return (col.name().to_owned(), val.unwrap().0);
                        } else {
                            return (
                                col.name().to_owned(),
                                serde_json::value::Value::from(text.to_owned()),
                            );
                        }
                    } else {
                        let val = row
                            .try_get_unchecked::<sqlx::types::Json<serde_json::value::Value>, _>(
                                col.name(),
                            );
                        return (col.name().to_owned(), val.unwrap().0);
                    }
                });
                serde_json::map::Map::from_iter(json_iter)
            })
            .collect()
    }
}

#[async_trait]
impl IntoConnection<rusqlite::Connection> for SqliteConnection {
    async fn into_connection(mut self) -> rusqlite::Connection {
        self.as_connection().await
    }
}

#[async_trait]
impl IntoConnection<rusqlite::Connection> for PoolConnection<Sqlite> {
    async fn into_connection(mut self) -> rusqlite::Connection {
        let mut handle = self.lock_handle().await.unwrap();
        unsafe { rusqlite::Connection::from_handle(handle.as_raw_handle().as_ptr()).unwrap() }
    }
}

#[async_trait]
impl IntoConnection<rusqlite::Connection> for rusqlite::Connection {
    async fn into_connection(mut self) -> rusqlite::Connection {
        self
    }
}

#[async_trait]
impl IntoConnection<rusqlite::Connection> for &SqlitePool {
    async fn into_connection(mut self) -> rusqlite::Connection {
        let conn = self.acquire().await.unwrap();
        conn.into_connection().await
    }
}

#[async_trait]
impl AsConnection<rusqlite::Connection> for SqliteConnection {
    async fn as_connection(&mut self) -> rusqlite::Connection {
        let mut handle = self.lock_handle().await.unwrap();
        unsafe { rusqlite::Connection::from_handle(handle.as_raw_handle().as_ptr()).unwrap() }
    }
}

impl SkaldHooks {
    pub async fn new(
        connection: impl IntoConnection<rusqlite::Connection>,
        instance: Instance,
    ) -> Self {
        let handler = SqliteConnectionHandler::new(connection.into_connection().await, instance);
        Self { handler }
    }

    pub fn with_table(
        self,
        database: String,
        table: String,
        settings: Vec<TableIndexSettings>,
    ) -> Self {
        Self {
            handler: self.handler.with_table(database, table, settings),
        }
    }

    pub fn build(
        self,
    ) -> impl Fn(&mut SqliteConnection, PoolConnectionMetadata) -> BoxFuture<'_, Result<(), sqlx::Error>>
    {
        let handler = Arc::new(self.handler);
        let conns = Arc::new(Mutex::new(vec![]));

        move |conn: &mut SqliteConnection, _: PoolConnectionMetadata| {
            let conns = conns.clone();
            let handler = handler.clone();

            Box::pin(async move {
                let rusqlite_conn = conn.as_connection().await;
                handler.attach_hooks(&rusqlite_conn);
                conns.lock().unwrap().push(rusqlite_conn);
                Ok(())
            })
        }
    }
}
