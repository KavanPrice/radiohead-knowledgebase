mod load_env_vars;

use crate::load_env_vars::EnvVars;
use futures_util::TryStreamExt;
use neo4rs;
use neo4rs::{query, Graph, Query};
use rspotify::clients::BaseClient;
use rspotify::model::AlbumType::Album;
use rspotify::model::{AlbumId, ArtistId, FullAlbum, FullArtist, SimplifiedAlbum};
use rspotify::{ClientCredsSpotify, ClientError, Credentials};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio;
use tokio::time::Instant;

#[tokio::main]
async fn main() {
    let env_vars = EnvVars::new();

    let spotify_client = ClientCredsSpotify::new(Credentials::new(
        &env_vars.client_id,
        &env_vars.client_secret,
    ));
    spotify_client.request_token().await.unwrap();

    let graph = Arc::new(
        Graph::new(
            env_vars.neo4j_uri,
            env_vars.neo4j_username,
            env_vars.neo4j_password,
        )
        .await
        .unwrap(),
    );

    let artist_uris = HashMap::from([
        ("radiohead", "spotify:artist:4Z8W4fKeB5YxbusRsdQVPb"),
        ("taylor_swift", "spotify:artist:06HL4z0CvFAxyc27GXpf02"),
        ("arctic_monkeys", "spotify:artist:7Ln80lUS6He07XvHI8qqHH"),
        ("tyler_the_creator", "spotify:artist:4V8LLVI7PbaPR0K2TGSxFF"),
        ("olivia_rodrigo", "spotify:artist:1McMsnEElThX1knmY4oliG"),
        ("daft_punk", "spotify:artist:4tZwfgrHOc3mvqYlEYSvVi"),
        ("snarky_puppy", "spotify:artist:7ENzCHnmJUr20nUjoZ0zZ1"),
    ]);

    let mut query_counter = 1;

    for (artist_name, uri) in artist_uris {
        let artist_start = Instant::now();
        let artist = get_artist_from_uri(&spotify_client, uri).await.unwrap();
        let artist_albums = get_artist_albums(&spotify_client, &artist).await.unwrap();

        let queries = construct_artist_queries(&artist, &artist_albums).await;

        for query in queries {
            let query_start = Instant::now();
            let mut query_vec: Vec<Query> = Vec::new();
            query_vec.push(query);

            match execute_query(&graph, query_vec).await {
                Err(e) => {
                    eprintln!("Error processing query: {}", e);
                    println!("Error processing query {} in ", query_counter);
                }
                Ok(()) => {
                    let query_duration = query_start.elapsed();
                    println!(
                        "Successfully processed query {} in time {:?}",
                        query_counter, query_duration
                    )
                }
            }
            query_counter += 1;
        }
        let artist_duration = artist_start.elapsed();
        println!("Completed {} in time {:?}", artist_name, artist_duration);
    }
}

async fn get_artist_from_uri(
    client: &ClientCredsSpotify,
    uri: &str,
) -> Result<FullArtist, ClientError> {
    let radiohead_id = ArtistId::from_uri(uri).unwrap();
    client.artist(radiohead_id).await
}

async fn get_artist_albums(
    client: &ClientCredsSpotify,
    artist: &FullArtist,
) -> Option<Vec<FullAlbum>> {
    let simplified_album_vec: Arc<Mutex<Vec<SimplifiedAlbum>>> = Arc::new(Mutex::new(Vec::new()));
    client
        .artist_albums(artist.id.clone(), [Album], None)
        .try_for_each_concurrent(10, |album| {
            let simplified_album_vec = Arc::clone(&simplified_album_vec);

            async move {
                let mut simplified_album_vec = simplified_album_vec.lock().unwrap();
                simplified_album_vec.push(album);
                Ok(())
            }
        })
        .await
        .unwrap();

    let album_ids: Vec<AlbumId> = simplified_album_vec
        .lock()
        .unwrap()
        .iter()
        .map(|album| album.id.clone().unwrap())
        .collect();
    let mut album_vec: Vec<FullAlbum> = Vec::new();
    for album_id in album_ids {
        album_vec.push(client.album(album_id, None).await.unwrap());
    }

    match album_vec.len() {
        0 => None,
        _ => Some(album_vec),
    }
}

async fn construct_artist_queries(artist: &FullArtist, albums: &Vec<FullAlbum>) -> Vec<Query> {
    let mut queries: Vec<Query> = Vec::new();
    let artist_id = artist.id.clone().to_string().replace('\'', "\\'");
    let artist_name = artist.name.clone().replace('\'', "\\'");

    queries.push(query(&format!(
        "MERGE (artist:Artist {{id: '{}', name: '{}'}})",
        artist_id, artist_name
    )));
    for genre in artist.genres.iter() {
        queries.push(query(&format!(
            "MERGE (artist:Artist {{id: '{}', name: '{}'}})\
            MERGE (genre:Genre {{name: '{}'}})\
            MERGE (artist)-[:genre]->(genre)",
            artist_id, artist_name, genre
        )));
    }
    for album in albums.iter() {
        let album_id = album.id.clone().to_string().replace('\'', "\\'");
        let album_name = album.name.clone().replace('\'', "\\'");

        queries.push(query(&format!(
            "MERGE (artist:Artist {{id: '{}', name: '{}'}})\
            MERGE (album:Album {{id: '{}', name: '{}'}})\
            MERGE (artist)-[:released]->(album)",
            artist_id, artist_name, album_id, album_name
        )));
        for genre in album.genres.iter() {
            queries.push(query(&format!(
                "MERGE (album:Album {{id: '{}', name: '{}'}})\
                MERGE (genre: Genre {{name: '{}'}})\
                MERGE (album)-[:has_genre]->(genre)",
                album.id, album.name, genre
            )));
        }
        for image in album.images.iter() {
            queries.push(query(&format!(
                "MERGE (album:Album {{id: '{}', name: '{}'}})\
                MERGE (image: Image {{url: '{}'}})\
                MERGE (album)-[:has_album_art]->(image)",
                album_id, album_name, image.url
            )));
        }
        for track in album.tracks.items.iter() {
            let track_id = track.id.clone().unwrap().to_string().replace('\'', "\\'");
            let track_name = track.name.clone().replace('\'', "\\'");

            for track_artist in track.artists.iter() {
                let track_artist_id = track_artist
                    .id
                    .clone()
                    .unwrap()
                    .to_string()
                    .replace('\'', "\\'");
                let track_artist_name = track_artist.name.clone().replace('\'', "\\'");

                queries.push(query(&format!(
                    "MERGE (artist:Artist {{id: '{}', name: '{}'}})\
                    MERGE (album:Album {{id: '{}', name: '{}'}})\
                    MERGE (track:Track {{id: '{}', name: '{}'}})\
                    MERGE (artist)-[:wrote]->(track)\
                    MERGE (album)-[:has_track]->(track)",
                    track_artist_id, track_artist_name, album_id, album_name, track_id, track_name
                )));
            }
        }
    }

    queries
}

async fn execute_query(graph: &Graph, query: Vec<Query>) -> Result<(), Box<dyn std::error::Error>> {
    let txn = graph.start_txn().await?;
    let result = txn.run_queries(query).await;
    if let Err(err) = result {
        eprintln!("Error executing query: {}", err);
    }
    txn.commit().await?;
    Ok(())
}
