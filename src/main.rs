use std::{
    collections::{HashMap, HashSet},
    io::{BufReader, BufWriter},
    sync::mpsc,
};

use anyhow::{bail, Context};
use clap::Parser;
use google_drive3::{
    hyper::{self, client::HttpConnector},
    hyper_rustls::{HttpsConnector, HttpsConnectorBuilder},
    oauth2::{self, InstalledFlowAuthenticator, InstalledFlowReturnMethod},
    DriveHub,
};
use log::{error, info, warn};
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::{serde_as, DisplayFromStr};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logger()?;
    let args = Args::parse();
    let drive = init_drive().await?;
    let ctrlc_handler = init_ctrlc()?;

    if args.list {
        list_files(&drive, &ctrlc_handler).await?;
    } else if args.show_overview {
        show_overview()?;
    } else if args.tree {
        show_tree()?;
    }

    Ok(())
}

#[derive(Parser)]
struct Args {
    #[clap(long)]
    list: bool,
    #[clap(long)]
    show_overview: bool,
    #[clap(long)]
    tree: bool,
}

#[derive(Serialize, Deserialize)]
struct FileList {
    files: Vec<File>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct File {
    id: String,
    #[serde(rename = "mimeType")]
    mime_type: String,
    #[serde(deserialize_with = "null_to_default")]
    parents: Vec<String>,
    name: String,
    #[serde(rename = "quotaBytesUsed")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    quota_bytes_used: Option<u64>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    size: Option<u64>,
    #[serde(rename = "sha256Checksum")]
    sha256_checksum: Option<String>,
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

fn init_logger() -> anyhow::Result<()> {
    use simplelog::*;
    CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Info,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        ),
        WriteLogger::new(
            LevelFilter::Info,
            Config::default(),
            fs_err::File::create("ignore/log.log").unwrap(),
        ),
    ])?;
    Ok(())
}

fn init_ctrlc() -> anyhow::Result<mpsc::Receiver<()>> {
    let (sender, receiver) = mpsc::channel();
    ctrlc::set_handler(move || {
        warn!("Ctrl-C detected!");
        if let Err(e) = sender.send(()) {
            error!("Failed to send ctrl-c signal.  Main thread dead?  {e}");
        }
    })?;
    Ok(receiver)
}

type Drive = DriveHub<HttpsConnector<HttpConnector>>;
async fn init_drive() -> anyhow::Result<Drive> {
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

fn restore_data(allow_not_found: bool) -> anyhow::Result<Vec<FileList>> {
    Ok(match fs_err::File::open("ignore/file-list.json") {
        Ok(file) => {
            let res: Vec<FileList> = serde_json::from_reader(BufReader::new(file))?;
            info!("Loaded {} pages", res.len());
            res
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && allow_not_found => {
            info!("Starting from scratch: {error} (not found)");
            vec![]
        }
        Err(e) => Err(e)?,
    })
}
fn save_data(list: &[FileList]) -> anyhow::Result<()> {
    (|| {
        let path = "ignore/file-list.json";
        let file = fs_err::File::create(path)?;
        serde_json::to_writer(BufWriter::new(file), list)?;
        info!("Saved list to {path:?}");
        anyhow::Ok(())
    })()
    .context(
        "Unfortunately, we failed to save data and the accumulated data was permanently losed.",
    )
}

async fn list_files(drive: &Drive, ctrlc_handler: &mpsc::Receiver<()>) -> anyhow::Result<()> {
    let mut list = restore_data(true)?;
    loop {
        let token = match list.last() {
            None => "",
            Some(last) => match &last.next_page_token {
                None => {
                    save_data(&list)?;
                    info!("Complete.");
                    break;
                }
                Some(ref token) => token,
            },
        };
        info!("Page {}", list.len());
        let Ok(res) = drive
            .files()
            .list()
            // Includes all owned files plus shared roots (not shared children)?
            .corpora("user") // "user" by default, but setting it explicitly
            .q("'me' in owners")
            .page_token(token)
            .param("fields", "nextPageToken,files(id,mimeType,parents,name,size,quotaBytesUsed,sha256Checksum)")
            .doit()
            .await else {
            error!("Aborting due to an API error.");
            break save_data(&list)?
        };
        let Ok(res) = FileList::try_from(res.1) else {
            error!("Aborting due to a conversion error.");
            break save_data(&list)?
        };
        list.push(res);
        if let Ok(()) = ctrlc_handler.try_recv() {
            info!("Received ctrl-c.  Saving before terminating.");
            save_data(&list)?;
            break;
        }
        if list.len() % 10 == 0 {
            save_data(&list)?;
        }
    }
    Ok(())
}

fn show_overview() -> anyhow::Result<()> {
    let list = restore_data(false)?;
    let files: Vec<_> = list.into_iter().flat_map(|e| e.files).collect();
    let sum: u64 = files.iter().filter_map(|f| f.quota_bytes_used).sum();
    println!("{sum}");

    let print_file =
        |file: &File| println!("{:?} {:50} {}", file.parents, file.mime_type, file.name);

    println!("=== Files without a parent (or with multiple parents) ===");
    for file in files.iter().filter(|f| f.parents.len() != 1) {
        print_file(file);
    }

    let ids: HashSet<&str> = files.iter().map(|f| &f.id as &str).collect();
    println!("=== Files with parents not owned by me ===");
    for file in files.iter().filter(|f| {
        f.parents.iter().any(|id| !ids.contains(id as &str))
            && f.quota_bytes_used.unwrap_or(0) > 1024
    }) {
        print_file(file);
    }

    Ok(())
}

fn show_tree() -> anyhow::Result<()> {
    let list = restore_data(false)?;
    let files: Vec<_> = list.into_iter().flat_map(|e| e.files).collect();

    let id_to_file: HashMap<_, _> = files.iter().map(|file| (&file.id as &str, file)).collect();
    let mut parent_id_to_children = HashMap::<_, Vec<_>>::new();
    for file in &files {
        match &file.parents[..] {
            [] => {}
            [parent] => parent_id_to_children
                .entry(parent as &str)
                .or_default()
                .push(file),
            _ => bail!("Multiple parents: {file:?}"),
        }
    }

    enum Node<'a> {
        File(&'a File),
        Root { id: &'a str, name: String },
    }
    fn dfs(id_to_children: &HashMap<&str, Vec<&File>>, this: Node, depth: usize) -> u64 {
        let mut size_sum = match this {
            Node::File(&File {
                quota_bytes_used: Some(bytes),
                ..
            }) => bytes,
            _ => 0,
        };
        let (id, name) = match this {
            Node::File(file) => (&file.id as &str, &file.name),
            Node::Root { id, ref name } => (id, name),
        };
        for child in id_to_children.get(id).iter().flat_map(|&x| x) {
            size_sum += dfs(id_to_children, Node::File(child), depth + 1);
        }
        if size_sum >= 50 * (1 << 20) {
            println!("{}o {}  {name}", " ".repeat(depth), format_size(size_sum));
        }
        size_sum
    }
    files
        .iter()
        .flat_map(|f| &f.parents)
        .filter_map(|id| match id_to_file.get(id as &str) {
            None => Some(Node::Root {
                id,
                name: format!("Root ({id})"),
            }),
            Some(file) => (file.parents.is_empty()).then_some(Node::File(file)),
        })
        .for_each(|file| {
            dfs(&parent_id_to_children, file, 0);
        });

    Ok(())
}

fn format_size(size: u64) -> String {
    let prefix = ["", "Ki", "Mi", "Gi"];
    prefix
        .iter()
        .enumerate()
        .rev()
        .find_map(|(i, prefix)| {
            let base = 1 << (i * 10);
            (size >= base).then(|| format!("{:.2} {prefix}B", size as f64 / base as f64))
        })
        .unwrap_or("0 B".into())
}
