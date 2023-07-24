use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Read,
    sync::mpsc,
    thread,
    time::Duration,
};

use r2d2::{ManageConnection, Pool};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{types::ValueRef, Connection, OpenFlags};
use skald::{
    embedded_milli::{IndexSettings, Instance},
    PrimaryKeyFn, SqliteMilliConnectionManager, TableIndexSettings,
};
use slite::Migrator;

fn main() {
    let mut buffer = String::new();
    File::open("./examples/artist.sql")
        .unwrap()
        .read_to_string(&mut buffer)
        .unwrap();

    let manager = SqliteConnectionManager::memory().with_flags(
        OpenFlags::default() | OpenFlags::SQLITE_OPEN_MEMORY | OpenFlags::SQLITE_OPEN_SHARED_CACHE,
    );
    let conn = manager.connect().unwrap();

    let migrator = Migrator::new(
        &[buffer],
        conn,
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
    wtxn.commit().unwrap();

    let manager = SqliteMilliConnectionManager::new(manager, instance).with_table(
        "artist".to_owned(),
        vec![TableIndexSettings {
            index_name: "artist".to_owned(),
            update_query: "select artist_id, artist_name, extra from artist where rowid = ?1"
                .to_owned(),
            primary_key_fn: PrimaryKeyFn::new(|accessor| {
                if let ValueRef::Integer(val) = accessor.get_old_column_value(0) {
                    val.to_string()
                } else {
                    unreachable!()
                }
            }),
        }],
    );
    let pool = Pool::new(manager).unwrap();
    let conn = pool.get().unwrap();
    conn.execute("insert into artist(artist_name, created_date, extra) values('test', DATE('now'), '{\"yo\":[true,2]}')", []).unwrap();

    thread::sleep(Duration::from_millis(1000));
    let rtxn = index.read();
    let res = index
        .search_documents(
            &rtxn,
            Some("'test'".to_owned()),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    println!("RES {res:?}");
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
