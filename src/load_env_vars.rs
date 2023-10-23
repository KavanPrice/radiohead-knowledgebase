use dotenv::dotenv;
use std::env;

pub struct EnvVars {
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
    pub neo4j_uri: String,
    pub neo4j_username: String,
    pub neo4j_password: String,
    pub aura_instance_id: String,
    pub aura_instance_name: String,
}

impl EnvVars {
    pub fn new() -> Self {
        dotenv().ok();

        Self {
            client_id: env::var("CLIENT_ID").unwrap_or_default(),
            client_secret: env::var("CLIENT_SECRET").unwrap_or_default(),
            redirect_uri: env::var("REDIRECT_URI").unwrap_or_default(),
            neo4j_uri: env::var("NEO4J_URI").unwrap_or_default(),
            neo4j_username: env::var("NEO4J_USERNAME").unwrap_or_default(),
            neo4j_password: env::var("NEO4J_PASSWORD").unwrap_or_default(),
            aura_instance_id: env::var("AURA_INSTANCEID").unwrap_or_default(),
            aura_instance_name: env::var("AURA_INSTANCENAME").unwrap_or_default(),
        }
    }
}
