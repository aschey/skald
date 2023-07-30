use skald::{
    embedded_milli::{IndexSettings, Instance},
    pool::sqlx::{IntoConnection, QueryExt, SkaldHooks},
    PrimaryKeyFn, TableIndexSettings,
};
use slite::Migrator;
use sqlx::{sqlite::SqlitePoolOptions, ValueRef};
use std::{fs::File, io::Read, thread, time::Duration};

#[tokio::main]
async fn main() {
    let mut buffer = String::new();
    File::open("./examples/artist.sql")
        .unwrap()
        .read_to_string(&mut buffer)
        .unwrap();

    let path = "sqlx.db";

    let pool = sqlx::SqlitePool::connect(path).await.unwrap();

    let migrator = Migrator::new(
        &[buffer],
        pool.into_connection().await,
        slite::Config::default(),
        slite::Options {
            allow_deletions: true,
            dry_run: false,
        },
    )
    .unwrap();
    migrator.migrate().unwrap();

    let instance = Instance::new("./test_index");

    let index = instance.get_index("artist").unwrap();
    let mut wtxn = index.write();

    index
        .set_settings(
            &mut wtxn,
            IndexSettings {
                primary_key: Some("artist_id".to_owned()),
                filterable_fields: vec!["artist_id".to_owned(), "artist_name".to_owned()],
                sortable_fields: vec!["artist_id".to_owned(), "artist_name".to_owned()],
                ..Default::default()
            },
        )
        .unwrap();

    sqlx::query("insert into artist(artist_name, created_date, extra) values('test2', DATE('now'), '{\"yo\":[true,2]}')").execute(&pool).await.unwrap();
    let res = sqlx::query("select artist_id, artist_name, extra from artist")
        .fetch_all(&pool)
        .await
        .unwrap();
    index.set_documents(&mut wtxn, res.query_to_json()).unwrap();
    wtxn.commit().unwrap();

    let rtxn = index.read();
    let res = index
        .search_documents(&rtxn, |search| {
            search.query("test2");
        })
        .unwrap();
    println!("RES0 {res:?}");

    let hooks = SkaldHooks::new(&pool, instance).await.with_table(
        "main".to_owned(),
        "artist".to_owned(),
        vec![TableIndexSettings {
            index_name: "artist".to_owned(),
            update_query: "select artist_id, artist_name, extra from artist where rowid = ?"
                .to_owned(),
            primary_key_fn: PrimaryKeyFn::new(|accessor| {
                if let rusqlite::types::ValueRef::Integer(val) = accessor.get_old_column_value(0) {
                    val.to_string()
                } else {
                    unreachable!()
                }
            }),
        }],
    );
    let pool = SqlitePoolOptions::default()
        .after_connect(hooks.build())
        .connect(path)
        .await
        .unwrap();

    sqlx::query("insert into artist(artist_name, created_date, extra) values('test', DATE('now'), '{\"yo\":[true,2]}')").execute(&pool).await.unwrap();

    thread::sleep(Duration::from_millis(100));
    let rtxn = index.read();

    let res = index
        .search_documents(&rtxn, |search| {
            search.query("test");
        })
        .unwrap();
    println!("RES1 {res:?}");
    sqlx::query("delete from artist")
        .execute(&pool)
        .await
        .unwrap();
    thread::sleep(Duration::from_millis(100));
    let rtxn = index.read();
    let res = index
        .search_documents(&rtxn, |search| {
            search.query("test");
        })
        .unwrap();
    println!("RES2 {res:?}");
}

// SELECT 'song' || s.song_id as entry_id, s.song_title entry, 'song' as entry_type, al.album_name album, ar.artist_name artist
// FROM song s
// INNER JOIN artist ar ON ar.artist_id = s.artist_id
// INNER JOIN album al ON al.album_id = s.album_id
// UNION ALL
// SELECT 'album' || al.album_id as entry_id, al.album_name entry, 'album' as entry_type, null as album, ar.artist_name artist
// FROM album al
// INNER JOIN artist ar ON ar.artist_id = al.artist_id
// UNION ALL
// SELECT 'artist'  || ar.artist_id as entry_id, ar.artist_name entry, 'artist' as entry_type, null as album, null as artist
// FROM artist ar
