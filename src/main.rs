mod load_env_vars;

use crate::load_env_vars::EnvVars;
use futures_util::TryStreamExt;
use neo4rs;
use neo4rs::{query, Graph, Query};
use rspotify::clients::BaseClient;
use rspotify::model::AlbumType::Album;
use rspotify::model::{AlbumId, ArtistId, FullAlbum, SimplifiedAlbum};
use rspotify::{ClientCredsSpotify, Credentials};
use std::sync::{Arc, Mutex};
use tokio;

#[tokio::main]
async fn main() {
    let env_vars = EnvVars::new();

    let spotify_client = ClientCredsSpotify::new(Credentials::new(
        &*env_vars.client_id,
        &*env_vars.client_secret,
    ));
    spotify_client.request_token().await.unwrap();

    let radiohead_uri = "spotify:artist:4Z8W4fKeB5YxbusRsdQVPb";
    let radiohead_id = ArtistId::from_uri(radiohead_uri).unwrap();
    let radiohead = spotify_client.artist(radiohead_id).await.unwrap();

    let simplified_album_vec: Arc<Mutex<Vec<SimplifiedAlbum>>> = Arc::new(Mutex::new(Vec::new()));
    spotify_client
        .artist_albums(radiohead.id, [Album], None)
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
        album_vec.push(spotify_client.album(album_id, None).await.unwrap());
    }

    let graph = Arc::new(
        Graph::new(
            env_vars.neo4j_uri,
            env_vars.neo4j_username,
            env_vars.neo4j_password,
        )
        .await
        .unwrap(),
    );

    let mut queries: Vec<Query> = Vec::new();
    queries.push(query(&format!(
        "MERGE (artist:Artist {{name: '{}'}})",
        radiohead.name
    )));
    for genre in radiohead.genres.iter() {
        queries.push(query(&format!(
            "MERGE (artist:Artist {{name: '{}'}})\
            MERGE (genre:Genre {{name: '{}'}})\
            MERGE (artist)-[:genre]->(genre)",
            radiohead.name, genre
        )));
    }
    for album in album_vec.iter() {
        queries.push(query(&format!(
            "MERGE (artist:Artist {{name: '{}'}})\
            MERGE (album:Album {{name: '{}'}})\
            MERGE (artist)-[:released]->(album)",
            radiohead.name, album.name
        )));
        for genre in album.genres.iter() {
            queries.push(query(&format!(
                "MERGE (album:Album {{name: '{}'}})\
                MERGE (genre: Genre {{name: '{}'}})\
                MERGE (album)-[:has_genre]->(genre)",
                album.name, genre
            )));
        }
        for image in album.images.iter() {
            queries.push(query(&format!(
                "MERGE (album:Album {{name: '{}'}})\
                MERGE (image: Image {{url: '{}'}})\
                MERGE (album)-[:has_album_art]->(image)",
                album.name, image.url
            )));
        }
        for track in album.tracks.items.iter() {
            for artist in track.artists.iter() {
                queries.push(query(&format!(
                    "MERGE (artist:Artist {{name: '{}'}})\
                    MERGE (album:Album {{name: '{}'}})\
                    MERGE (track:Track {{name: '{}'}})\
                    MERGE (artist)-[:wrote]->(track)\
                    MERGE (album)-[:has_track]->(track)",
                    artist.name, album.name, track.name
                )));
            }
        }
    }

    for query in queries {
        let mut query_vec: Vec<Query> = Vec::new();
        query_vec.push(query);
        if let Err(err) = execute_query(&graph, query_vec).await {
            eprintln!("Error processing query: {}", err);
        }
    }
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
