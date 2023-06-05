use google_drive3::{
    hyper::{self, client::HttpConnector},
    hyper_rustls::{HttpsConnector, HttpsConnectorBuilder},
    oauth2::{self, InstalledFlowAuthenticator, InstalledFlowReturnMethod},
    DriveHub,
};
use serde::{Deserialize, Deserializer, Serialize};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let drive = init_drive().await?;

    let res = drive
        .files()
        .list()
        // Includes all owned files plus shared roots (not shared children)?
        .corpora("user") // "user" by default, but setting it explicitly
        .q("'me' in owners")
        .param("fields", "nextPageToken,files(id,mimeType,parents,name)")
        .doit()
        .await?;
    let res: FileList = serde_json::from_str(&serde_json::to_string(&res.1).unwrap()).unwrap();
    for file in res.files {
        println!(
            "{:44}  {:50} {:30?} {}",
            file.id, file.mime_type, file.parents, file.name
        );
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct FileList {
    files: Vec<File>,
    next_page_token: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct File {
    id: String,
    #[serde(rename = "mimeType")]
    mime_type: String,
    #[serde(deserialize_with = "null_to_default")]
    parents: Vec<String>,
    name: String,
}
fn null_to_default<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    let opt = Option::deserialize(d)?;
    let val = opt.unwrap_or_default();
    Ok(val)
}

async fn init_drive() -> anyhow::Result<DriveHub<HttpsConnector<HttpConnector>>> {
    let hyper = hyper::Client::builder().build(
        HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_only()
            .enable_http2()
            .build(),
    );
    let auth = {
        let secret = oauth2::read_application_secret("ignore/clientsecret.json").await?;
        InstalledFlowAuthenticator::builder(secret, InstalledFlowReturnMethod::HTTPRedirect)
            .persist_tokens_to_disk("ignore/tokencache.json")
            .build()
            .await?
    };
    Ok(DriveHub::new(hyper, auth))
}
