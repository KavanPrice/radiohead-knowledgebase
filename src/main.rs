mod load_env_vars;

use crate::load_env_vars::EnvVars;
use futures_util::TryStreamExt;
use neo4rs::{query, Graph, Node, Query};
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
    let mut unexpanded_artist_uris = Arc::new(Mutex::new(HashMap::from([
        (
            String::from("spotify:artist:4Z8W4fKeB5YxbusRsdQVPb"),
            String::from("radiohead"),
        ),
        (
            String::from("spotify:artist:6styCzc1Ej4NxISL0LiigM"),
            String::from("the_smile"),
        ),
        (
            String::from("spotify:artist:7tA9Eeeb68kkiG9Nrvuzmi"),
            String::from("atoms_for_peace"),
        ),
    ])));

    let mut query_counter = 1;

    for i in 0..2 {
        expand_graph(
            &spotify_client,
            &graph,
            &mut unexpanded_artist_uris,
            &mut query_counter,
        )
        .await
        .unwrap();
    }
}

async fn expand_graph(
    client: &ClientCredsSpotify,
    graph: &Arc<Graph>,
    unexpanded_uris: &mut Arc<Mutex<HashMap<String, String>>>,
    query_counter: &mut i32,
) -> Result<(), ClientError> {
    let mut keys_to_remove: Vec<String> = Vec::new();

    let mut unexpanded_uris_locked = unexpanded_uris.lock().unwrap();

    for (uri, artist_name) in unexpanded_uris_locked.iter() {
        let artist_start = Instant::now();
        let artist = get_artist_from_uri(client, uri).await?;
        let artist_albums = get_artist_albums(client, &artist).await.unwrap();
        let queries = construct_artist_queries(&artist, &artist_albums).await;

        for query in queries {
            let query_start = Instant::now();
            let query_vec: Vec<Query> = vec![query];

            match execute_merge_query(graph, query_vec).await {
                Err(e) => {
                    eprintln!("Error processing query: {}", e);
                    println!("Error processing query {} in ", *query_counter);
                }
                Ok(()) => {
                    let query_duration = query_start.elapsed();
                    println!(
                        "Successfully processed query {} in time {:?}",
                        *query_counter, query_duration
                    )
                }
            }
            *query_counter += 1;
        }
        let artist_duration = artist_start.elapsed();
        println!("Completed {} in time {:?}", artist_name, artist_duration);
        keys_to_remove.push(uri.to_owned());
    }

    let formatted_uris = format!(
        "\'{}\'",
        unexpanded_uris_locked
            .keys()
            .map(|uri| uri.to_owned())
            .collect::<Vec<String>>()
            .join("\', \'")
    );

    let mut remaining_artists_result = graph
        .execute(query(&format!(
            "MATCH (artist:Artist) WHERE NOT artist.uri IN [{}] RETURN artist",
            formatted_uris
        )))
        .await
        .unwrap();

    while let Ok(Some(row)) = remaining_artists_result.next().await {
        let node: Node = row.get("artist").unwrap();
        let id = node.get::<String>("uri").unwrap();
        let name = node.get::<String>("name").unwrap();
        unexpanded_uris_locked.insert(id, name);
    }

    for key in keys_to_remove {
        unexpanded_uris_locked.remove(&key);
    }

    Ok(())
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

async fn construct_artist_queries(artist: &FullArtist, albums: &[FullAlbum]) -> Vec<Query> {
    let mut queries: Vec<Query> = Vec::new();
    let artist_id = artist.id.clone().to_string().replace('\'', "\\'");
    let artist_name = artist.name.clone().replace('\'', "\\'");

    queries.push(query(&format!(
        "MERGE (artist:Artist {{uri: '{}', name: '{}'}})",
        artist_id, artist_name
    )));
    for genre in artist.genres.iter() {
        queries.push(query(&format!(
            "MERGE (artist:Artist {{uri: '{}', name: '{}'}})\
            MERGE (genre:Genre {{name: '{}'}})\
            MERGE (artist)-[:genre]->(genre)",
            artist_id, artist_name, genre
        )));
    }
    for album in albums.iter() {
        let album_id = album.id.clone().to_string().replace('\'', "\\'");
        let album_name = album.name.clone().replace('\'', "\\'");

        queries.push(query(&format!(
            "MERGE (artist:Artist {{uri: '{}', name: '{}'}})\
            MERGE (album:Album {{uri: '{}', name: '{}'}})\
            MERGE (artist)-[:released]->(album)",
            artist_id, artist_name, album_id, album_name
        )));
        for genre in album.genres.iter() {
            queries.push(query(&format!(
                "MERGE (album:Album {{uri: '{}', name: '{}'}})\
                MERGE (genre: Genre {{name: '{}'}})\
                MERGE (album)-[:has_genre]->(genre)",
                album.id, album.name, genre
            )));
        }
        for image in album.images.iter() {
            queries.push(query(&format!(
                "MERGE (album:Album {{uri: '{}', name: '{}'}})\
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
                    "MERGE (artist:Artist {{uri: '{}', name: '{}'}})\
                    MERGE (album:Album {{uri: '{}', name: '{}'}})\
                    MERGE (track:Track {{uri: '{}', name: '{}'}})\
                    MERGE (artist)-[:wrote]->(track)\
                    MERGE (album)-[:has_track]->(track)",
                    track_artist_id, track_artist_name, album_id, album_name, track_id, track_name
                )));
            }
        }
    }

    queries
}

async fn execute_merge_query(
    graph: &Graph,
    query: Vec<Query>,
) -> Result<(), Box<dyn std::error::Error>> {
    let txn = graph.start_txn().await?;
    let result = txn.run_queries(query).await;
    if let Err(err) = result {
        eprintln!("Error executing query: {}", err);
    }
    txn.commit().await?;
    Ok(())
}
