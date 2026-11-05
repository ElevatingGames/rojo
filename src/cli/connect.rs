//! Implements `rojo connect`, which makes the Studio plugin start a sync
//! session as if the user had clicked "Connect" in Studio.
//!
//! The server running `rojo serve` brokers the request (see `web::connect`).
//! This command creates the request, then polls it to report the plugin's
//! progress, and answers for the user when the plugin needs confirmation.

use std::{
    io::{self, BufRead, IsTerminal, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    path::PathBuf,
    sync::mpsc::{self, Receiver, TryRecvError},
    thread,
    time::{Duration, Instant},
};

use anyhow::{bail, format_err, Context};
use clap::Parser;
use memofs::Vfs;
use reqwest::{blocking::RequestBuilder, StatusCode};
use serde::{de::DeserializeOwned, Serialize};
use termcolor::{Color, ColorSpec, StandardStream, WriteColor};
use thiserror::Error;

use crate::{
    project::Project,
    web::{
        interface::{
            ChangeEntry, ChangeKind, ChangeSummary, ConnectDecision, ConnectDecisionRequest,
            ConnectDecisionResponse, ConnectRequest, ConnectRequestId, ConnectResponse,
            ConnectState, ConnectStatusResponse, ErrorResponse,
        },
        util::{deserialize_msgpack, serialize_msgpack},
    },
};

use super::{
    resolve_path,
    serve::{DEFAULT_BIND_ADDRESS, DEFAULT_PORT},
    GlobalOptions,
};

/// How often to check on the request.
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// How long to wait before pointing out what might be wrong if no plugin has
/// picked up the request yet.
const HINT_DELAY: Duration = Duration::from_secs(5);

/// The plugin reports back as soon as it claims a request, so if it hasn't
/// after this long, it has likely gone away.
const PLUGIN_ACK_TIMEOUT: Duration = Duration::from_secs(15);

/// Once connecting, the plugin may go quiet while it works out what changed in
/// a large place. If it's quiet for longer than this, give up on it.
const PLUGIN_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How many changes to list when asking for confirmation, unless `--verbose`.
const SHOWN_CHANGES: usize = 25;

/// How many changed properties to list for one instance.
const SHOWN_PROPERTIES: usize = 6;

/// Make the Rojo Studio plugin connect to a running `rojo serve`, like clicking
/// "Connect" in Studio.
///
/// If Studio has changes that would normally need confirming before syncing,
/// they are listed and you are asked whether to continue. Without a terminal
/// to ask in, Rojo does not connect unless `--force` is given.
#[derive(Debug, Parser)]
pub struct ConnectCommand {
    /// Path to the project being served, used to find the server's address and
    /// port. Defaults to the current directory.
    #[clap(default_value = "")]
    pub project: PathBuf,

    /// The address of the Rojo server. Defaults to the project's
    /// `serveAddress`, or localhost.
    #[clap(long)]
    pub address: Option<String>,

    /// The port of the Rojo server. Defaults to the project's `servePort`, or
    /// `34872`.
    #[clap(long)]
    pub port: Option<u16>,

    /// Sync without asking, even if Studio has changes that would normally
    /// need confirming. Those changes in Studio will be overwritten.
    #[clap(long, short)]
    pub force: bool,

    /// Only connect a Studio window that has this place open.
    #[clap(long)]
    pub place_id: Option<u64>,

    /// How many seconds to wait for Studio to pick up the request.
    #[clap(long, default_value = "30")]
    pub timeout: u64,
}

impl ConnectCommand {
    pub fn run(self, global: GlobalOptions) -> anyhow::Result<()> {
        let (host, port) = self.server_host_and_port()?;
        let client = ApiClient::new(&host, port)?;

        let response: ConnectResponse = client
            .post(
                "/api/connect",
                &ConnectRequest {
                    force: self.force,
                    place_id: self.place_id,
                },
            )
            .map_err(|err| describe_create_error(err, &client))?;

        let mut out = StandardStream::stdout(global.color.into());

        if response.already_connected {
            print_labeled(
                &mut out,
                Color::Green,
                "Already connected:",
                &format!("Studio is syncing project '{}'.", response.project_name),
            )?;
            return Ok(());
        }

        let request_id = response
            .request_id
            .context("The Rojo server did not return a connect request ID")?;

        writeln!(
            out,
            "Asking Studio to connect to project '{}' at {}...",
            response.project_name, client.display_address
        )?;

        Waiter {
            client: &client,
            out,
            request_id,
            project_name: response.project_name,
            force: self.force,
            place_id: self.place_id,
            timeout: Duration::from_secs(self.timeout),
            verbose: global.verbosity > 0,
            started: Instant::now(),
            shown_hint: false,
            announced_plugin: false,
            last_message: None,
            shown_changes: false,
            prompt: None,
            abort_reason: None,
        }
        .wait()
    }

    fn server_host_and_port(&self) -> anyhow::Result<(String, u16)> {
        let project = if self.address.is_some() && self.port.is_some() {
            None
        } else {
            self.load_project()?
        };

        let port = self
            .port
            .or_else(|| project.as_ref().and_then(|project| project.serve_port))
            .unwrap_or(DEFAULT_PORT);

        let host = match &self.address {
            Some(address) => address.clone(),
            None => {
                let bind_address = project
                    .as_ref()
                    .and_then(|project| project.serve_address)
                    .unwrap_or(DEFAULT_BIND_ADDRESS.into());

                connectable_address(bind_address).to_string()
            }
        };

        Ok((host, port))
    }

    /// Loads the project to read its serve settings from. Not finding one is
    /// only an error if a path was given explicitly.
    fn load_project(&self) -> anyhow::Result<Option<Project>> {
        let path_given = !self.project.as_os_str().is_empty();
        let path = resolve_path(&self.project)?;
        let vfs = Vfs::new_default()?;

        match Project::load_fuzzy(&vfs, &path) {
            Ok(Some(project)) => Ok(Some(project)),
            Ok(None) if path_given => bail!("No project file was found at {}", path.display()),
            Ok(None) => Ok(None),
            Err(err) => Err(err)
                .with_context(|| format!("Could not load the project at {}", path.display())),
        }
    }
}

/// Picks the address to reach a server bound to `bind_address` from this
/// machine.
fn connectable_address(bind_address: IpAddr) -> IpAddr {
    match bind_address {
        IpAddr::V4(ip) if ip.is_unspecified() => Ipv4Addr::LOCALHOST.into(),
        IpAddr::V6(ip) if ip.is_unspecified() => Ipv6Addr::LOCALHOST.into(),
        ip => ip,
    }
}

#[derive(Debug, Clone, Copy)]
enum AbortReason {
    /// The user said no at the prompt.
    Declined,
    /// There was no terminal to ask in, and `--force` wasn't given.
    NotInteractive,
}

/// Follows a connect request through to the end.
struct Waiter<'a> {
    client: &'a ApiClient,
    out: StandardStream,
    request_id: ConnectRequestId,
    project_name: String,
    force: bool,
    place_id: Option<u64>,
    timeout: Duration,
    verbose: bool,

    started: Instant,
    shown_hint: bool,
    announced_plugin: bool,
    last_message: Option<String>,
    shown_changes: bool,
    /// Receives the user's answer to the confirmation prompt, if it's open.
    prompt: Option<Receiver<Option<bool>>>,
    abort_reason: Option<AbortReason>,
}

impl Waiter<'_> {
    fn wait(mut self) -> anyhow::Result<()> {
        loop {
            let status: ConnectStatusResponse = self
                .client
                .get(&format!("/api/connect/{}", self.request_id))
                .context("Lost contact with the Rojo server")?;

            if status.state != ConnectState::Confirming {
                self.close_prompt()?;
            }

            match status.state {
                ConnectState::Pending => {
                    if self.on_pending()? {
                        continue;
                    }
                }
                ConnectState::Connecting => {
                    self.announce_plugin(&status)?;
                    self.show_progress(&status)?;
                    check_plugin_alive(&status)?;
                }
                ConnectState::Confirming => {
                    self.announce_plugin(&status)?;
                    self.on_confirming(&status)?;
                    check_plugin_alive(&status)?;
                }
                ConnectState::Connected => {
                    print_labeled(
                        &mut self.out,
                        Color::Green,
                        "Connected!",
                        &format!("Studio is now syncing project '{}'.", self.project_name),
                    )?;
                    return Ok(());
                }
                ConnectState::Aborted => return Err(self.aborted_error(&status)),
                ConnectState::Failed => bail!(
                    "Studio could not connect: {}",
                    status
                        .message
                        .as_deref()
                        .unwrap_or("no reason was given. Check Studio's Output window.")
                ),
                ConnectState::Cancelled => {
                    bail!("The connect request was cancelled before Studio picked it up.")
                }
            }

            thread::sleep(POLL_INTERVAL);
        }
    }

    /// Handles a request no plugin has picked up yet. Returns true if the
    /// request should be checked again right away.
    fn on_pending(&mut self) -> anyhow::Result<bool> {
        let elapsed = self.started.elapsed();

        if elapsed >= self.timeout {
            let status: ConnectStatusResponse = self
                .client
                .post(&format!("/api/connect/{}/cancel", self.request_id), &())
                .context("Lost contact with the Rojo server")?;

            if matches!(
                status.state,
                ConnectState::Pending | ConnectState::Cancelled
            ) {
                return Err(self.no_plugin_error(&status));
            }

            // A plugin picked up the request just before we withdrew it.
            return Ok(true);
        }

        if !self.shown_hint && elapsed >= HINT_DELAY {
            self.shown_hint = true;
            writeln!(
                self.out,
                "Still waiting. Make sure Studio is open, and the Rojo plugin's address and port \
                 are set to {}.",
                self.client.display_address
            )?;
        }

        Ok(false)
    }

    fn announce_plugin(&mut self, status: &ConnectStatusResponse) -> io::Result<()> {
        if self.announced_plugin {
            return Ok(());
        }
        self.announced_plugin = true;

        let place = status.plugin.as_ref().map(|plugin| {
            match (plugin.place_name.as_deref(), plugin.place_id) {
                (Some(name), Some(id)) => format!(" (place '{name}', ID {id})"),
                (Some(name), None) => format!(" (place '{name}')"),
                (None, Some(id)) => format!(" (place ID {id})"),
                (None, None) => String::new(),
            }
        });

        writeln!(
            self.out,
            "Studio{} picked up the request.",
            place.unwrap_or_default()
        )
    }

    fn show_progress(&mut self, status: &ConnectStatusResponse) -> io::Result<()> {
        if status.message.is_none() || status.message == self.last_message {
            return Ok(());
        }
        self.last_message = status.message.clone();

        if let Some(message) = &status.message {
            self.out.set_color(
                ColorSpec::new()
                    .set_fg(Some(Color::Black))
                    .set_intense(true),
            )?;
            writeln!(self.out, "  {message}")?;
            self.out.reset()?;
        }

        Ok(())
    }

    fn on_confirming(&mut self, status: &ConnectStatusResponse) -> anyhow::Result<()> {
        if !self.shown_changes {
            self.shown_changes = true;
            self.print_changes(status.changes.as_ref())?;

            if self.force {
                // The plugin accepts forced requests without asking, but answer
                // anyway in case it asked.
                self.decide(ConnectDecision::Accept)?;
            } else if io::stdin().is_terminal() {
                write!(
                    self.out,
                    "Accept these changes and connect? You can also answer in Studio. [y/N] "
                )?;
                self.out.flush()?;
                self.prompt = Some(spawn_prompt());
            } else {
                self.abort_reason = Some(AbortReason::NotInteractive);
                self.decide(ConnectDecision::Abort)?;
            }
        }

        let answer = match &self.prompt {
            Some(prompt) => match prompt.try_recv() {
                Ok(answer) => answer,
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => None,
            },
            None => return Ok(()),
        };
        self.prompt = None;

        let decision = if answer == Some(true) {
            ConnectDecision::Accept
        } else {
            if answer.is_none() {
                // Reading the answer failed, so the prompt's line was never
                // finished.
                writeln!(self.out)?;
            }
            self.abort_reason = Some(AbortReason::Declined);
            ConnectDecision::Abort
        };

        if !self.decide(decision)? {
            // Studio was answered first, so our answer didn't count.
            self.abort_reason = None;
        }

        Ok(())
    }

    /// Closes the confirmation prompt if it's still open, which happens when
    /// the confirmation was answered in Studio.
    fn close_prompt(&mut self) -> io::Result<()> {
        if self.prompt.take().is_some() {
            writeln!(self.out)?;
            writeln!(self.out, "Answered in Studio.")?;
        }
        Ok(())
    }

    /// Sends a decision on the changes awaiting confirmation. Returns whether
    /// it was applied.
    fn decide(&self, decision: ConnectDecision) -> anyhow::Result<bool> {
        let response: ConnectDecisionResponse = self
            .client
            .post(
                &format!("/api/connect/{}/decision", self.request_id),
                &ConnectDecisionRequest { decision },
            )
            .context("Lost contact with the Rojo server")?;

        Ok(response.applied)
    }

    fn print_changes(&mut self, changes: Option<&ChangeSummary>) -> io::Result<()> {
        let out = &mut self.out;

        writeln!(out)?;
        out.set_color(ColorSpec::new().set_fg(Some(Color::Yellow)).set_bold(true))?;
        write!(out, "Warning:")?;
        out.reset()?;
        writeln!(
            out,
            " Studio differs from project '{}'. Connecting will make these changes in Studio, \
             which may overwrite work done there since your last sync:",
            self.project_name
        )?;

        let Some(changes) = changes else {
            writeln!(out, "  (Studio did not send a list of the changes.)")?;
            writeln!(out)?;
            return Ok(());
        };

        let shown = if self.verbose {
            changes.entries.len()
        } else {
            changes.entries.len().min(SHOWN_CHANGES)
        };

        for entry in &changes.entries[..shown] {
            print_change(out, entry)?;
        }

        let total = (changes.total_entries as usize).max(changes.entries.len());
        if total > shown {
            let hint = if shown < changes.entries.len() {
                " (run with --verbose to list more, or see Studio for the full list)"
            } else {
                " (see Studio for the full list)"
            };
            writeln!(out, "  ...and {} more{hint}", total - shown)?;
        }

        writeln!(out)?;
        writeln!(
            out,
            "  {} added, {} removed, {} updated",
            plural(changes.added, "instance"),
            changes.removed,
            changes.updated
        )?;
        writeln!(out)
    }

    fn aborted_error(&self, status: &ConnectStatusResponse) -> anyhow::Error {
        match self.abort_reason {
            Some(AbortReason::NotInteractive) => format_err!(
                "Studio has changes that need confirming before syncing (listed above), so Rojo \
                 did not connect.\n\
                 Re-run with --force to sync anyway, or run `rojo connect` in a terminal to be \
                 asked."
            ),
            Some(AbortReason::Declined) => {
                format_err!("Did not connect. Studio was left unchanged.")
            }
            None => match &status.message {
                Some(message) => format_err!("The connection was aborted in Studio: {message}"),
                None => format_err!("The connection was aborted in Studio."),
            },
        }
    }

    fn no_plugin_error(&self, status: &ConnectStatusResponse) -> anyhow::Error {
        let mut message = format!(
            "No Studio plugin picked up the connect request within {} seconds.",
            self.timeout.as_secs()
        );

        if !status.ineligible_place_ids.is_empty() {
            let ids: Vec<String> = status
                .ineligible_place_ids
                .iter()
                .map(|id| id.to_string())
                .collect();

            message.push_str(&format!(
                "\nStudio is open with place ID {}, but ",
                ids.join(", ")
            ));
            match self.place_id {
                Some(wanted) => message.push_str(&format!("--place-id {wanted} was given.")),
                None => message.push_str(
                    "the project's servePlaceIds or blockedPlaceIds don't allow syncing to it.",
                ),
            }
        } else {
            message.push_str(&format!(
                "\nMake sure that:\
                 \n  - Roblox Studio is open, with the Rojo plugin from this build of Rojo \
                 installed (run `rojo plugin install`)\
                 \n  - the plugin isn't already connecting, and its 'Allow CLI Connect' setting \
                 is on\
                 \n  - the plugin's address and port are set to {}",
                self.client.display_address
            ));
        }

        format_err!(message)
    }
}

fn print_change(out: &mut StandardStream, entry: &ChangeEntry) -> io::Result<()> {
    let (symbol, color) = match entry.kind {
        ChangeKind::Add => ('+', Color::Green),
        ChangeKind::Remove => ('-', Color::Red),
        ChangeKind::Update => ('~', Color::Yellow),
    };

    out.set_color(ColorSpec::new().set_fg(Some(color)).set_bold(true))?;
    write!(out, "  {symbol} ")?;
    out.reset()?;
    write!(out, "{}", entry.path)?;

    out.set_color(
        ColorSpec::new()
            .set_fg(Some(Color::Black))
            .set_intense(true),
    )?;
    if !entry.class_name.is_empty() {
        write!(out, " ({})", entry.class_name)?;
    }
    if entry.descendants > 0 {
        write!(out, " and {}", plural(entry.descendants, "descendant"))?;
    }
    out.reset()?;

    if !entry.properties.is_empty() {
        let shown = entry.properties.len().min(SHOWN_PROPERTIES);
        write!(out, ": {}", entry.properties[..shown].join(", "))?;
        if entry.properties.len() > shown {
            write!(out, ", and {} more", entry.properties.len() - shown)?;
        }
    }

    writeln!(out)
}

fn plural(count: u32, noun: &str) -> String {
    if count == 1 {
        format!("{count} {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

fn print_labeled(
    out: &mut StandardStream,
    color: Color,
    label: &str,
    message: &str,
) -> io::Result<()> {
    out.set_color(ColorSpec::new().set_fg(Some(color)).set_bold(true))?;
    write!(out, "{label}")?;
    out.reset()?;
    writeln!(out, " {message}")
}

/// Reads the answer to the confirmation prompt on another thread, so that the
/// request can still be followed in case it's answered in Studio instead.
/// Sends `None` if the answer couldn't be read.
fn spawn_prompt() -> Receiver<Option<bool>> {
    let (sender, receiver) = mpsc::channel();

    thread::spawn(move || {
        let mut line = String::new();
        let answer = match io::stdin().lock().read_line(&mut line) {
            Ok(0) | Err(_) => None,
            Ok(_) => Some(matches!(
                line.trim().to_ascii_lowercase().as_str(),
                "y" | "yes"
            )),
        };

        let _ = sender.send(answer);
    });

    receiver
}

fn check_plugin_alive(status: &ConnectStatusResponse) -> anyhow::Result<()> {
    let limit = if status.plugin_acknowledged {
        PLUGIN_IDLE_TIMEOUT
    } else {
        PLUGIN_ACK_TIMEOUT
    };

    if let Some(idle) = status.secs_since_plugin_activity {
        if idle > limit.as_secs_f64() {
            bail!(
                "Studio stopped responding while connecting. Check Studio's Output window for \
                 errors."
            );
        }
    }

    Ok(())
}

fn describe_create_error(err: ApiError, client: &ApiClient) -> anyhow::Error {
    match err {
        ApiError::Unreachable(source) => format_err!(
            "Could not reach a Rojo server at {}. Is `rojo serve` running?\n\n{source}",
            client.display_address
        ),
        ApiError::Status {
            status: StatusCode::NOT_FOUND,
            ..
        } => format_err!(
            "The Rojo server at {} does not support `rojo connect`. Restart it using this build \
             of Rojo ({}).",
            client.display_address,
            env!("CARGO_PKG_VERSION")
        ),
        err => err.into(),
    }
}

#[derive(Debug, Error)]
enum ApiError {
    #[error("could not reach the Rojo server")]
    Unreachable(#[source] reqwest::Error),

    #[error("the Rojo server responded with {status}: {details}")]
    Status { status: StatusCode, details: String },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// A small client for the connect routes of the serve API.
struct ApiClient {
    client: reqwest::blocking::Client,
    base_url: String,
    /// The server's address as a user would type it into the Studio plugin.
    display_address: String,
}

impl ApiClient {
    fn new(host: &str, port: u16) -> anyhow::Result<Self> {
        let url_host = match host.parse::<IpAddr>() {
            Ok(IpAddr::V6(ip)) => format!("[{ip}]"),
            _ => host.to_owned(),
        };

        let display_host = match host.parse::<IpAddr>() {
            Ok(ip) if ip.is_loopback() => "localhost".to_owned(),
            _ => host.to_owned(),
        };

        let client = reqwest::blocking::Client::builder()
            // The server is usually on this machine, which a system proxy
            // couldn't reach on our behalf.
            .no_proxy()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("Could not create an HTTP client")?;

        Ok(Self {
            client,
            base_url: format!("http://{url_host}:{port}"),
            display_address: format!("{display_host}:{port}"),
        })
    }

    fn get<R: DeserializeOwned>(&self, path: &str) -> Result<R, ApiError> {
        self.send(self.client.get(format!("{}{path}", self.base_url)))
    }

    fn post<B: Serialize, R: DeserializeOwned>(&self, path: &str, body: &B) -> Result<R, ApiError> {
        let body = serialize_msgpack(body)?;
        self.send(
            self.client
                .post(format!("{}{path}", self.base_url))
                .body(body),
        )
    }

    fn send<R: DeserializeOwned>(&self, request: RequestBuilder) -> Result<R, ApiError> {
        let response = request.send().map_err(ApiError::Unreachable)?;
        let status = response.status();
        let body = response.bytes().map_err(ApiError::Unreachable)?;

        if status.is_success() {
            deserialize_msgpack(&body)
                .context("The Rojo server sent a response that could not be read")
                .map_err(ApiError::Other)
        } else {
            let details = match deserialize_msgpack::<ErrorResponse>(&body) {
                Ok(error) => error.details().to_owned(),
                Err(_) => String::from_utf8_lossy(&body).into_owned(),
            };

            Err(ApiError::Status { status, details })
        }
    }
}
