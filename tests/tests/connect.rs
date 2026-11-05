//! Tests for `rojo connect`. These run the real CLI against a real `rojo serve`,
//! with the test playing the part of the Studio plugin.

use std::{
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use hyper_tungstenite::tungstenite::connect;
use serde::{de::DeserializeOwned, Serialize};

use librojo::web_api::{
    ChangeEntry, ChangeKind, ChangeSummary, ClaimedConnectRequest, ConnectDecision,
    ConnectDecisionPollResponse, ConnectPollRequest, ConnectPollResponse, ConnectRequestId,
    ConnectState, ConnectStatusUpdate, ConnectStatusUpdateResponse,
};

use crate::rojo_test::{
    io_util::ROJO_PATH,
    serve_util::{deserialize_msgpack, run_serve_test, serialize_msgpack, TestServeSession},
};

/// How long to let `rojo connect` run before deciding it's stuck.
const CLI_TIMEOUT: Duration = Duration::from_secs(60);

#[test]
fn force_connects_without_confirmation() {
    run_serve_test("empty", |session, _| {
        let plugin = FakePlugin::new(&session, Some("1234"));
        let cli = spawn_connect(&session, &["--force"]);

        let claim = plugin.claim();
        assert!(claim.force, "--force should be passed on to the plugin");

        plugin.report(
            claim.request_id,
            1,
            ConnectState::Connecting,
            Some("Connecting to server..."),
            None,
        );
        plugin.report(claim.request_id, 2, ConnectState::Connected, None, None);

        let output = finish(cli);
        let stdout = String::from_utf8_lossy(&output.stdout);

        assert!(output.status.success(), "rojo connect failed: {output:?}");
        assert!(stdout.contains("(place 'Test Place', ID 1234)"), "{stdout}");
        assert!(stdout.contains("Connecting to server..."), "{stdout}");
        assert!(
            stdout.contains("Studio is now syncing project 'empty'"),
            "{stdout}"
        );
    });
}

#[test]
fn changes_abort_without_terminal() {
    run_serve_test("empty", |session, _| {
        let plugin = FakePlugin::new(&session, None);
        let cli = spawn_connect(&session, &[]);

        let claim = plugin.claim();
        assert!(!claim.force);

        plugin.report(
            claim.request_id,
            1,
            ConnectState::Confirming,
            None,
            Some(example_changes()),
        );

        // With no terminal to ask in, the CLI should abort on its own.
        let decision = plugin.wait_for_decision(claim.request_id);
        assert_eq!(decision, Some(ConnectDecision::Abort));

        plugin.report(
            claim.request_id,
            2,
            ConnectState::Aborted,
            Some("Aborted Rojo sync operation"),
            None,
        );

        let output = finish(cli);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(!output.status.success(), "rojo connect should fail");
        assert!(stdout.contains("Warning:"), "{stdout}");
        assert!(
            stdout.contains("+ Workspace.NewFolder (Folder) and 1 descendant"),
            "{stdout}"
        );
        assert!(stdout.contains("- Workspace.OldPart (Part)"), "{stdout}");
        assert!(
            stdout.contains("~ ReplicatedStorage.Module (ModuleScript): Source"),
            "{stdout}"
        );
        assert!(
            stdout.contains("2 instances added, 1 removed, 1 updated"),
            "{stdout}"
        );
        assert!(stderr.contains("--force"), "{stderr}");
    });
}

#[test]
fn force_accepts_confirmation() {
    run_serve_test("empty", |session, _| {
        let plugin = FakePlugin::new(&session, None);
        let cli = spawn_connect(&session, &["--force"]);

        // The plugin normally accepts forced requests by itself, but the CLI
        // should accept for it if it asks anyway.
        let claim = plugin.claim();
        plugin.report(
            claim.request_id,
            1,
            ConnectState::Confirming,
            None,
            Some(example_changes()),
        );

        let decision = plugin.wait_for_decision(claim.request_id);
        assert_eq!(decision, Some(ConnectDecision::Accept));

        plugin.report(claim.request_id, 2, ConnectState::Connected, None, None);

        let output = finish(cli);
        assert!(output.status.success(), "rojo connect failed: {output:?}");
    });
}

#[test]
fn plugin_failure_is_reported() {
    run_serve_test("empty", |session, _| {
        let plugin = FakePlugin::new(&session, None);
        let cli = spawn_connect(&session, &[]);

        let claim = plugin.claim();
        plugin.report(
            claim.request_id,
            1,
            ConnectState::Failed,
            Some("Cannot sync a model as a place."),
            None,
        );

        let output = finish(cli);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(!output.status.success(), "rojo connect should fail");
        assert!(
            stderr.contains("Cannot sync a model as a place."),
            "{stderr}"
        );
    });
}

#[test]
fn times_out_without_plugin() {
    run_serve_test("empty", |session, _| {
        let output = finish(spawn_connect(&session, &["--timeout", "1"]));
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(!output.status.success(), "rojo connect should fail");
        assert!(stderr.contains("No Studio plugin picked up"), "{stderr}");
    });
}

#[test]
fn reports_places_that_cannot_connect() {
    run_serve_test("empty", |session, _| {
        // This plugin's place doesn't match --place-id, so it should be passed
        // over. The poll is left to time out on its own.
        let port = session.port();
        thread::spawn(move || {
            let _ = reqwest::blocking::Client::new()
                .post(format!("http://localhost:{port}/api/connect/poll"))
                .body(serialize_msgpack(poll_request(Some("222"))).unwrap())
                .send();
        });

        let output = finish(spawn_connect(
            &session,
            &["--place-id", "111", "--timeout", "2"],
        ));
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(!output.status.success(), "rojo connect should fail");
        assert!(stderr.contains("place ID 222"), "{stderr}");
        assert!(stderr.contains("--place-id 111"), "{stderr}");
    });
}

#[test]
fn already_connected() {
    run_serve_test("empty", |session, _| {
        let url = format!("ws://localhost:{}/api/socket/0", session.port());
        let (socket, _response) = connect(url).expect("Couldn't open WebSocket");

        let output = finish(spawn_connect(&session, &[]));
        let stdout = String::from_utf8_lossy(&output.stdout);

        assert!(output.status.success(), "rojo connect failed: {output:?}");
        assert!(stdout.contains("Already connected:"), "{stdout}");

        drop(socket);
    });
}

fn example_changes() -> ChangeSummary {
    ChangeSummary {
        added: 2,
        removed: 1,
        updated: 1,
        entries: vec![
            ChangeEntry {
                kind: ChangeKind::Update,
                path: "ReplicatedStorage.Module".to_owned(),
                class_name: "ModuleScript".to_owned(),
                properties: vec!["Source".to_owned()],
                descendants: 0,
            },
            ChangeEntry {
                kind: ChangeKind::Add,
                path: "Workspace.NewFolder".to_owned(),
                class_name: "Folder".to_owned(),
                properties: Vec::new(),
                descendants: 1,
            },
            ChangeEntry {
                kind: ChangeKind::Remove,
                path: "Workspace.OldPart".to_owned(),
                class_name: "Part".to_owned(),
                properties: Vec::new(),
                descendants: 0,
            },
        ],
        total_entries: 3,
    }
}

fn spawn_connect(session: &TestServeSession, args: &[&str]) -> Child {
    let port = session.port().to_string();

    Command::new(ROJO_PATH)
        .arg("connect")
        .arg(session.path())
        .args(["--port", port.as_str(), "--color", "never"])
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Couldn't start rojo connect")
}

/// Waits for `rojo connect` to exit, killing it if it takes too long.
fn finish(mut child: Child) -> Output {
    let start = Instant::now();

    while child
        .try_wait()
        .expect("Couldn't wait on rojo connect")
        .is_none()
    {
        if start.elapsed() > CLI_TIMEOUT {
            let _ = child.kill();
            let output = child.wait_with_output().unwrap();
            panic!("rojo connect did not exit in time: {output:?}");
        }

        thread::sleep(Duration::from_millis(50));
    }

    child.wait_with_output().unwrap()
}

/// Does what the Studio plugin does when `rojo connect` is run.
struct FakePlugin {
    port: usize,
    place_id: Option<&'static str>,
}

impl FakePlugin {
    fn new(session: &TestServeSession, place_id: Option<&'static str>) -> Self {
        Self {
            port: session.port(),
            place_id,
        }
    }

    /// Polls until there's a request to claim.
    fn claim(&self) -> ClaimedConnectRequest {
        for _ in 0..3 {
            if let Some(claim) = poll(self.port, self.place_id).request {
                return claim;
            }
        }

        panic!("No connect request was made");
    }

    fn report(
        &self,
        id: ConnectRequestId,
        seq: u32,
        state: ConnectState,
        message: Option<&str>,
        changes: Option<ChangeSummary>,
    ) {
        let _: ConnectStatusUpdateResponse = post(
            self.port,
            &format!("/api/connect/{id}/status"),
            &ConnectStatusUpdate {
                seq,
                state,
                message: message.map(str::to_owned),
                changes,
            },
        );
    }

    /// Waits for `rojo connect` to accept or abort the changes awaiting
    /// confirmation.
    fn wait_for_decision(&self, id: ConnectRequestId) -> Option<ConnectDecision> {
        for _ in 0..3 {
            let response: ConnectDecisionPollResponse =
                get(self.port, &format!("/api/connect/{id}/decision"));

            if response.decision.is_some() {
                return response.decision;
            }
        }

        None
    }
}

fn poll(port: usize, place_id: Option<&str>) -> ConnectPollResponse {
    post(port, "/api/connect/poll", &poll_request(place_id))
}

fn poll_request(place_id: Option<&str>) -> ConnectPollRequest {
    ConnectPollRequest {
        place_id: place_id.map(str::to_owned),
        place_name: Some("Test Place".to_owned()),
    }
}

fn get<R: DeserializeOwned>(port: usize, path: &str) -> R {
    let response = reqwest::blocking::Client::new()
        .get(format!("http://localhost:{port}{path}"))
        .timeout(Duration::from_secs(30))
        .send()
        .expect("Request failed");

    decode(response)
}

fn post<B: Serialize, R: DeserializeOwned>(port: usize, path: &str, body: &B) -> R {
    let response = reqwest::blocking::Client::new()
        .post(format!("http://localhost:{port}{path}"))
        .body(serialize_msgpack(body).unwrap())
        .timeout(Duration::from_secs(30))
        .send()
        .expect("Request failed");

    decode(response)
}

fn decode<R: DeserializeOwned>(response: reqwest::blocking::Response) -> R {
    let status = response.status();
    let body = response.bytes().unwrap();
    assert!(
        status.is_success(),
        "Request failed with {status}: {body:?}"
    );

    deserialize_msgpack(&body).expect("Server returned malformed response")
}
