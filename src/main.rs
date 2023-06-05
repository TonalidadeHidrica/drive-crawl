use google_drive3::{
    hyper,
    hyper_rustls::HttpsConnectorBuilder,
    oauth2::{self, InstalledFlowAuthenticator, InstalledFlowReturnMethod},
    DriveHub,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let drive = {
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
        DriveHub::new(hyper, auth)
    };
    let res = drive
        .files()
        .list()
        .corpora("user") // "user" by default, but setting it explicitly
        // Includes all owned files plus shared roots (not shared children)?
        .param("fields", "nextPageToken,files(id,mimeType,parents,name)")
        .doit()
        .await?;
    for file in res.1.files.unwrap() {
        println!(
            "{:44}  {:50} {:30?} {}",
            file.id.unwrap(),
            file.mime_type.unwrap(),
            file.parents,
            file.name.unwrap()
        );
    }
    Ok(())
}
