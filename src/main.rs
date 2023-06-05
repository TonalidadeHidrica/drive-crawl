use std::io::{BufReader, BufWriter};

use anyhow::Context;
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

    let mut list = {
        match fs_err::File::open("ignore/file-list.json") {
            Ok(file) => {
                let res: Vec<FileList> = serde_json::from_reader(BufReader::new(file))?;
                println!("Loaded {} pages", res.len());
                res
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                println!("Starting from scratch: {error} (not found)");
                vec![]
            }
            Err(e) => Err(e)?,
        }
    };
    let save = |list: &[FileList]| {
        (|| {
            let path = "ignore/file-list.json";
            let file = fs_err::File::create(path)?;
            serde_json::to_writer(BufWriter::new(file), list)?;
            println!("Saved list to {path:?}");
            anyhow::Ok(())
        })()
        .context(
            "Unfortunately, we failed to save data and the accumulated data was permanently losed.",
        )
    };
    loop {
        let token = match list.last() {
            None => "",
            Some(last) => match &last.next_page_token {
                None => break println!("Complete."),
                Some(ref token) => token,
            },
        };
        println!("Page {}", list.len());
        let Ok(res) = drive
            .files()
            .list()
            // Includes all owned files plus shared roots (not shared children)?
            .corpora("user") // "user" by default, but setting it explicitly
            .q("'me' in owners")
            .page_token(token)
            .param("fields", "nextPageToken,files(id,mimeType,parents,name)")
            .doit()
            .await else { break save(&list)? };
        let Ok(res) = FileList::try_from(res.1) else { break save(&list)? };
        list.push(res);
        if list.len() % 10 == 0 {
            save(&list)?;
        }
    }
    save(&list)?;
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct FileList {
    files: Vec<File>,
    #[serde(rename="nextPageToken")]
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
impl TryFrom<google_drive3::api::FileList> for FileList {
    type Error = anyhow::Error;
    fn try_from(value: google_drive3::api::FileList) -> anyhow::Result<Self> {
        Ok(serde_json::from_str(&serde_json::to_string(&value)?)?)
    }
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
