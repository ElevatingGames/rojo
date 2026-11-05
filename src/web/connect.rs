//! Lets `rojo connect` ask the Studio plugin to start a sync session, as if
//! the user had clicked "Connect" in Studio.
//!
//! The plugin can only make outgoing requests, so the server brokers the
//! handoff: `rojo connect` creates a request, and idle plugins long-poll for
//! one to claim. The plugin that claims it connects as usual and reports its
//! progress back here, including any changes that need confirming, which the
//! CLI reads by polling the request's status.
//!
//! Routes used by `rojo connect`, which only accept clients on the same machine
//! as the server:
//!
//! * `POST /api/connect`: create a request.
//! * `GET /api/connect/{id}`: read a request's status.
//! * `POST /api/connect/{id}/decision`: accept or abort changes awaiting
//!   confirmation.
//! * `POST /api/connect/{id}/cancel`: withdraw a request no plugin has claimed.
//!
//! Routes used by the plugin:
//!
//! * `POST /api/connect/poll`: wait for a request to claim.
//! * `POST /api/connect/{id}/status`: report progress on a claimed request.
//! * `GET /api/connect/{id}/decision`: wait for the CLI to accept or abort.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::{Duration, Instant},
};

use hyper::{body, Body, Method, Request, Response, StatusCode};
use serde::de::DeserializeOwned;
use tokio::sync::watch;

use crate::{
    serve_session::ServeSession,
    web::{
        interface::{
            ChangeSummary, ClaimedConnectRequest, ConnectDecision, ConnectDecisionPollResponse,
            ConnectDecisionRequest, ConnectDecisionResponse, ConnectPluginInfo, ConnectPollRequest,
            ConnectPollResponse, ConnectRequest, ConnectRequestId, ConnectResponse, ConnectState,
            ConnectStatusResponse, ConnectStatusUpdate, ConnectStatusUpdateResponse, ErrorResponse,
        },
        origin::canonical,
        util::{deserialize_msgpack, msgpack, msgpack_ok},
    },
};

/// How long a plugin's poll waits for a request before returning empty-handed.
/// This is kept well below Studio's HTTP timeout, so that the server never
/// hands a request to a poll whose plugin has already given up on it.
const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(10);

/// Pending requests are only handed out while the `rojo connect` that made
/// them is still polling for their status. Otherwise a `rojo connect` that was
/// interrupted could make Studio connect at some arbitrary later time.
const CLI_ABANDON_TIMEOUT: Duration = Duration::from_secs(5);

/// How long requests are kept after reaching a final state.
const FINISHED_RETENTION: Duration = Duration::from_secs(5 * 60);

/// Requests that never reach a final state, for example because Studio was
/// closed partway through connecting, are dropped after this long.
const MAX_REQUEST_AGE: Duration = Duration::from_secs(60 * 60);

/// Tracks connect requests made by `rojo connect` while the server runs.
pub struct ConnectBroker {
    records: Mutex<HashMap<ConnectRequestId, Record>>,

    /// Wakes long-polling plugins whenever a request changes.
    changed: watch::Sender<()>,

    /// The number of plugins currently subscribed over a WebSocket.
    open_sockets: AtomicUsize,
}

/// Outcome of [`ConnectBroker::create`].
pub enum CreateOutcome {
    Created(ConnectRequestId),
    AlreadyConnected,
}

/// Rejects a plugin's status update.
pub enum UpdateError {
    NotFound,
    InvalidState(ConnectState),
}

/// Holds open a WebSocket subscription's place in
/// [`ConnectBroker::open_sockets`] until dropped.
pub struct SocketGuard(Arc<ConnectBroker>);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        self.0.open_sockets.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The places a project allows syncing to, from `servePlaceIds` and
/// `blockedPlaceIds`.
pub struct PlaceFilter {
    serve_place_ids: Option<HashSet<u64>>,
    blocked_place_ids: Option<HashSet<u64>>,
}

impl PlaceFilter {
    pub fn from_session(session: &ServeSession) -> Self {
        Self {
            serve_place_ids: session.serve_place_ids().cloned(),
            blocked_place_ids: session.blocked_place_ids().cloned(),
        }
    }

    fn allows(&self, place_id: u64) -> bool {
        self.serve_place_ids
            .as_ref()
            .is_none_or(|ids| ids.contains(&place_id))
            && self
                .blocked_place_ids
                .as_ref()
                .is_none_or(|ids| !ids.contains(&place_id))
    }
}

struct Record {
    force: bool,
    place_id: Option<u64>,

    state: ConnectState,
    message: Option<String>,
    changes: Option<ChangeSummary>,
    decision: Option<ConnectDecision>,

    plugin: Option<ConnectPluginInfo>,
    plugin_acknowledged: bool,
    last_seq: Option<u32>,
    ineligible_place_ids: Vec<u64>,

    created_at: Instant,
    last_cli_activity: Instant,
    last_plugin_activity: Option<Instant>,
    finished_at: Option<Instant>,
}

impl Record {
    fn new(request: ConnectRequest, now: Instant) -> Self {
        Self {
            force: request.force,
            place_id: request.place_id,
            state: ConnectState::Pending,
            message: None,
            changes: None,
            decision: None,
            plugin: None,
            plugin_acknowledged: false,
            last_seq: None,
            ineligible_place_ids: Vec::new(),
            created_at: now,
            last_cli_activity: now,
            last_plugin_activity: None,
            finished_at: None,
        }
    }

    /// Whether a plugin with the given place open may claim this request.
    fn accepts(&self, place_id: Option<u64>, filter: &PlaceFilter) -> bool {
        match place_id {
            Some(place_id) => {
                self.place_id.is_none_or(|wanted| wanted == place_id) && filter.allows(place_id)
            }
            // We can't check a place we don't know, so leave it to the plugin's
            // own place ID checks unless the CLI asked for a specific place.
            None => self.place_id.is_none(),
        }
    }

    fn is_claimable(&self, now: Instant) -> bool {
        self.state == ConnectState::Pending
            && now.saturating_duration_since(self.last_cli_activity) < CLI_ABANDON_TIMEOUT
    }

    fn finish(&mut self, state: ConnectState, now: Instant) {
        self.state = state;
        if state.is_final() {
            self.finished_at = Some(now);
        }
    }

    fn to_response(&self, request_id: ConnectRequestId, now: Instant) -> ConnectStatusResponse {
        ConnectStatusResponse {
            request_id,
            state: self.state,
            message: self.message.clone(),
            changes: self.changes.clone(),
            plugin: self.plugin.clone(),
            plugin_acknowledged: self.plugin_acknowledged,
            secs_since_plugin_activity: self
                .last_plugin_activity
                .map(|at| now.saturating_duration_since(at).as_secs_f64()),
            ineligible_place_ids: self.ineligible_place_ids.clone(),
        }
    }
}

impl Default for ConnectBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectBroker {
    pub fn new() -> Self {
        let (changed, _) = watch::channel(());

        Self {
            records: Mutex::new(HashMap::new()),
            changed,
            open_sockets: AtomicUsize::new(0),
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<ConnectRequestId, Record>> {
        // A panic while holding the lock can't leave a record half-updated in
        // a way that matters, so keep serving rather than poisoning the API.
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn notify(&self) {
        self.changed.send_replace(());
    }

    /// Counts a plugin as connected for as long as the returned guard lives.
    pub fn track_socket(self: &Arc<Self>) -> SocketGuard {
        self.open_sockets.fetch_add(1, Ordering::SeqCst);
        SocketGuard(Arc::clone(self))
    }

    pub fn create(&self, request: ConnectRequest) -> CreateOutcome {
        if self.open_sockets.load(Ordering::SeqCst) > 0 {
            return CreateOutcome::AlreadyConnected;
        }

        let now = Instant::now();
        let id = ConnectRequestId::new_v4();

        {
            let mut records = self.lock();
            prune(&mut records, now);
            records.insert(id, Record::new(request, now));
        }

        self.notify();
        CreateOutcome::Created(id)
    }

    pub fn status(&self, id: ConnectRequestId) -> Option<ConnectStatusResponse> {
        let now = Instant::now();
        let mut records = self.lock();
        let record = records.get_mut(&id)?;
        record.last_cli_activity = now;

        Some(record.to_response(id, now))
    }

    /// Records the CLI's decision on changes awaiting confirmation. Returns
    /// whether it was applied, which it isn't if the request is no longer
    /// waiting for one.
    pub fn decide(&self, id: ConnectRequestId, decision: ConnectDecision) -> Option<bool> {
        let applied = {
            let mut records = self.lock();
            let record = records.get_mut(&id)?;
            record.last_cli_activity = Instant::now();

            let applied = record.state == ConnectState::Confirming && record.decision.is_none();
            if applied {
                record.decision = Some(decision);
            }
            applied
        };

        if applied {
            self.notify();
        }
        Some(applied)
    }

    /// Withdraws a request if no plugin has claimed it yet, or aborts it if it's
    /// waiting for confirmation. Requests further along are left alone.
    pub fn cancel(&self, id: ConnectRequestId) -> Option<ConnectStatusResponse> {
        let now = Instant::now();
        let response = {
            let mut records = self.lock();
            let record = records.get_mut(&id)?;
            record.last_cli_activity = now;

            match record.state {
                ConnectState::Pending => record.finish(ConnectState::Cancelled, now),
                ConnectState::Confirming if record.decision.is_none() => {
                    record.decision = Some(ConnectDecision::Abort)
                }
                _ => {}
            }

            record.to_response(id, now)
        };

        self.notify();
        Some(response)
    }

    /// Waits for a request this plugin may claim, claiming it if one arrives
    /// before the poll times out.
    pub async fn poll(
        &self,
        plugin: ConnectPluginInfo,
        filter: PlaceFilter,
    ) -> Option<ClaimedConnectRequest> {
        // Subscribe before checking so that a request created in between still
        // wakes us up.
        let mut changed = self.changed.subscribe();
        let deadline = tokio::time::Instant::now() + LONG_POLL_TIMEOUT;

        loop {
            if let Some(claim) = self.try_claim(&plugin, &filter) {
                return Some(claim);
            }

            match tokio::time::timeout_at(deadline, changed.changed()).await {
                Ok(Ok(())) => continue,
                _ => return None,
            }
        }
    }

    fn try_claim(
        &self,
        plugin: &ConnectPluginInfo,
        filter: &PlaceFilter,
    ) -> Option<ClaimedConnectRequest> {
        let now = Instant::now();
        let mut records = self.lock();

        // Of the requests this plugin may claim, take the oldest. Note any it
        // can't claim, so the CLI can explain why nothing picked them up.
        let mut oldest: Option<(ConnectRequestId, Instant)> = None;
        for (id, record) in records.iter_mut() {
            if !record.is_claimable(now) {
                continue;
            }

            if record.accepts(plugin.place_id, filter) {
                if oldest.is_none_or(|(_, created_at)| record.created_at < created_at) {
                    oldest = Some((*id, record.created_at));
                }
            } else if let Some(place_id) = plugin.place_id {
                if !record.ineligible_place_ids.contains(&place_id) {
                    record.ineligible_place_ids.push(place_id);
                }
            }
        }

        let (id, _) = oldest?;
        let record = records.get_mut(&id)?;
        record.state = ConnectState::Connecting;
        record.plugin = Some(plugin.clone());
        record.last_plugin_activity = Some(now);

        Some(ClaimedConnectRequest {
            request_id: id,
            force: record.force,
        })
    }

    /// Applies a status update from the plugin that claimed a request.
    pub fn update(
        &self,
        id: ConnectRequestId,
        update: ConnectStatusUpdate,
    ) -> Result<(), UpdateError> {
        if matches!(
            update.state,
            ConnectState::Pending | ConnectState::Cancelled
        ) {
            return Err(UpdateError::InvalidState(update.state));
        }

        let now = Instant::now();
        {
            let mut records = self.lock();
            let record = records.get_mut(&id).ok_or(UpdateError::NotFound)?;
            record.last_plugin_activity = Some(now);

            // Updates are sent concurrently, so they may arrive out of order.
            // Drop any that are older than one already applied, as well as any
            // for a request that has already finished.
            let is_stale = record.last_seq.is_some_and(|last| update.seq <= last);
            if is_stale || record.state.is_final() || record.state == ConnectState::Pending {
                return Ok(());
            }

            record.last_seq = Some(update.seq);
            record.plugin_acknowledged = true;
            record.message = update.message;

            if update.state == ConnectState::Confirming && record.state != ConnectState::Confirming
            {
                record.decision = None;
            }
            if update.changes.is_some() {
                record.changes = update.changes;
            }

            record.finish(update.state, now);
        }

        self.notify();
        Ok(())
    }

    /// Waits for the CLI to decide on changes awaiting confirmation. Returns
    /// early, without a decision, if the request stops waiting for one.
    pub async fn wait_for_decision(
        &self,
        id: ConnectRequestId,
    ) -> Option<ConnectDecisionPollResponse> {
        let mut changed = self.changed.subscribe();
        let deadline = tokio::time::Instant::now() + LONG_POLL_TIMEOUT;

        loop {
            let (response, waiting) = self.check_decision(id)?;
            if !waiting {
                return Some(response);
            }

            match tokio::time::timeout_at(deadline, changed.changed()).await {
                Ok(Ok(())) => continue,
                _ => return Some(response),
            }
        }
    }

    /// Returns a request's decision status and whether it's still waiting for
    /// a decision.
    fn check_decision(&self, id: ConnectRequestId) -> Option<(ConnectDecisionPollResponse, bool)> {
        let mut records = self.lock();
        let record = records.get_mut(&id)?;
        record.last_plugin_activity = Some(Instant::now());

        let waiting = record.state == ConnectState::Confirming && record.decision.is_none();
        let response = ConnectDecisionPollResponse {
            state: record.state,
            decision: record.decision,
        };

        Some((response, waiting))
    }
}

/// Drops requests that no one will ask about again, and cancels pending ones
/// whose CLI has gone away.
fn prune(records: &mut HashMap<ConnectRequestId, Record>, now: Instant) {
    for record in records.values_mut() {
        if record.state == ConnectState::Pending && !record.is_claimable(now) {
            record.finish(ConnectState::Cancelled, now);
        }
    }

    records.retain(|_, record| match record.finished_at {
        Some(finished_at) => now.saturating_duration_since(finished_at) < FINISHED_RETENTION,
        None => now.saturating_duration_since(record.created_at) < MAX_REQUEST_AGE,
    });
}

/// Whether a peer is on the same machine as the server: either it connected
/// over loopback, or it connected to one of the server's own addresses from
/// that same address.
///
/// As with `/api/open`, a connection forwarded over loopback by a tunnel or
/// proxy counts as local. Standing up that tunnel delegates trust to whatever
/// is on the other end of it.
fn is_local_peer(remote_addr: SocketAddr, local_addr: SocketAddr) -> bool {
    let remote = canonical(remote_addr.ip());
    remote.is_loopback() || remote == canonical(local_addr.ip())
}

/// Returns whether a path is handled by this module.
pub fn handles_path(path: &str) -> bool {
    path == "/api/connect" || path.starts_with("/api/connect/")
}

pub async fn call(
    serve_session: Arc<ServeSession>,
    broker: Arc<ConnectBroker>,
    remote_addr: SocketAddr,
    local_addr: SocketAddr,
    request: Request<Body>,
) -> Response<Body> {
    route(&serve_session, &broker, remote_addr, local_addr, request)
        .await
        .unwrap_or_else(Rejection::into_response)
}

async fn route(
    serve_session: &ServeSession,
    broker: &ConnectBroker,
    remote_addr: SocketAddr,
    local_addr: SocketAddr,
    request: Request<Body>,
) -> Result<Response<Body>, Rejection> {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let segments: Vec<&str> = path
        .strip_prefix("/api/connect")
        .unwrap_or_default()
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();

    // Requests from `rojo connect` drive the user's Studio session, so they're
    // only accepted from the machine running the server.
    let is_cli_route = match (&method, segments.as_slice()) {
        (&Method::POST, ["poll"]) => false,
        (&Method::POST, []) | (&Method::GET, [_]) => true,
        (&Method::POST, [_, "decision" | "cancel"]) => true,
        _ => false,
    };

    if is_cli_route && !is_local_peer(remote_addr, local_addr) {
        return Err(Rejection::forbidden(
            "rojo connect is only available to clients on the same machine as the server",
        ));
    }

    match (&method, segments.as_slice()) {
        (&Method::POST, []) => {
            // An empty body asks for the defaults, which keeps this route easy
            // to call from other tools.
            let body: ConnectRequest = read_body_or_default(request).await?;

            let (already_connected, request_id) = match broker.create(body) {
                CreateOutcome::Created(id) => (false, Some(id)),
                CreateOutcome::AlreadyConnected => (true, None),
            };

            Ok(msgpack_ok(ConnectResponse {
                session_id: serve_session.session_id(),
                project_name: serve_session.project_name().to_owned(),
                already_connected,
                request_id,
            }))
        }

        (&Method::POST, ["poll"]) => {
            let body: ConnectPollRequest = read_body(request).await?;
            let plugin = ConnectPluginInfo {
                place_id: body.place_id.and_then(|id| id.trim().parse().ok()),
                place_name: body.place_name,
            };

            let request = broker
                .poll(plugin, PlaceFilter::from_session(serve_session))
                .await;

            Ok(msgpack_ok(ConnectPollResponse { request }))
        }

        (&Method::GET, [id]) => {
            let status = broker
                .status(parse_id(id)?)
                .ok_or_else(Rejection::unknown_request)?;
            Ok(msgpack_ok(status))
        }

        (&Method::POST, [id, "cancel"]) => {
            let status = broker
                .cancel(parse_id(id)?)
                .ok_or_else(Rejection::unknown_request)?;
            Ok(msgpack_ok(status))
        }

        (&Method::POST, [id, "decision"]) => {
            let id = parse_id(id)?;
            let body: ConnectDecisionRequest = read_body(request).await?;
            let applied = broker
                .decide(id, body.decision)
                .ok_or_else(Rejection::unknown_request)?;

            Ok(msgpack_ok(ConnectDecisionResponse { applied }))
        }

        (&Method::POST, [id, "status"]) => {
            let id = parse_id(id)?;
            let body: ConnectStatusUpdate = read_body(request).await?;

            match broker.update(id, body) {
                Ok(()) => Ok(msgpack_ok(ConnectStatusUpdateResponse {})),
                Err(UpdateError::NotFound) => Err(Rejection::unknown_request()),
                Err(UpdateError::InvalidState(state)) => Err(Rejection::bad_request(format!(
                    "Plugins cannot set a request's state to {state}"
                ))),
            }
        }

        (&Method::GET, [id, "decision"]) => {
            let response = broker
                .wait_for_decision(parse_id(id)?)
                .await
                .ok_or_else(Rejection::unknown_request)?;

            Ok(msgpack_ok(response))
        }

        _ => Err(Rejection {
            status: StatusCode::NOT_FOUND,
            error: ErrorResponse::not_found(format!("Route not found: {path}")),
        }),
    }
}

/// A request that was turned away, to be sent back as an error response.
struct Rejection {
    status: StatusCode,
    error: ErrorResponse,
}

impl Rejection {
    fn bad_request(details: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error: ErrorResponse::bad_request(details),
        }
    }

    fn forbidden(details: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            error: ErrorResponse::forbidden(details),
        }
    }

    fn unknown_request() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error: ErrorResponse::not_found("No connect request with that ID"),
        }
    }

    fn into_response(self) -> Response<Body> {
        msgpack(self.error, self.status)
    }
}

fn parse_id(id: &str) -> Result<ConnectRequestId, Rejection> {
    ConnectRequestId::from_str(id)
        .map_err(|_| Rejection::bad_request("Malformed connect request ID"))
}

async fn read_body<T: DeserializeOwned>(request: Request<Body>) -> Result<T, Rejection> {
    let bytes = read_bytes(request).await?;
    decode(&bytes)
}

async fn read_body_or_default<T: DeserializeOwned + Default>(
    request: Request<Body>,
) -> Result<T, Rejection> {
    let bytes = read_bytes(request).await?;
    if bytes.is_empty() {
        Ok(T::default())
    } else {
        decode(&bytes)
    }
}

async fn read_bytes(request: Request<Body>) -> Result<body::Bytes, Rejection> {
    body::to_bytes(request.into_body())
        .await
        .map_err(|err| Rejection::bad_request(format!("Could not read body: {err}")))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Rejection> {
    deserialize_msgpack(bytes).map_err(|err| Rejection::bad_request(format!("Invalid body: {err}")))
}

#[cfg(test)]
mod test {
    use super::*;

    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    fn no_filter() -> PlaceFilter {
        PlaceFilter {
            serve_place_ids: None,
            blocked_place_ids: None,
        }
    }

    fn plugin(place_id: Option<u64>) -> ConnectPluginInfo {
        ConnectPluginInfo {
            place_id,
            place_name: None,
        }
    }

    fn created(outcome: CreateOutcome) -> ConnectRequestId {
        match outcome {
            CreateOutcome::Created(id) => id,
            CreateOutcome::AlreadyConnected => panic!("expected a request to be created"),
        }
    }

    fn update(seq: u32, state: ConnectState) -> ConnectStatusUpdate {
        ConnectStatusUpdate {
            seq,
            state,
            message: None,
            changes: None,
        }
    }

    #[test]
    fn claim_and_connect() {
        let broker = ConnectBroker::new();
        let id = created(broker.create(ConnectRequest {
            force: true,
            place_id: None,
        }));

        let claim = broker.try_claim(&plugin(Some(5)), &no_filter()).unwrap();
        assert_eq!(claim.request_id, id);
        assert!(claim.force);

        // A request can only be claimed once.
        assert!(broker.try_claim(&plugin(Some(5)), &no_filter()).is_none());

        let status = broker.status(id).unwrap();
        assert_eq!(status.state, ConnectState::Connecting);
        assert!(!status.plugin_acknowledged);

        assert!(broker
            .update(id, update(1, ConnectState::Connected))
            .is_ok());
        let status = broker.status(id).unwrap();
        assert_eq!(status.state, ConnectState::Connected);
        assert!(status.plugin_acknowledged);
    }

    #[test]
    fn place_id_filters() {
        let broker = ConnectBroker::new();
        let id = created(broker.create(ConnectRequest {
            force: false,
            place_id: Some(10),
        }));

        assert!(broker.try_claim(&plugin(Some(11)), &no_filter()).is_none());
        assert!(broker.try_claim(&plugin(None), &no_filter()).is_none());
        assert_eq!(broker.status(id).unwrap().ineligible_place_ids, vec![11]);

        let blocked = PlaceFilter {
            serve_place_ids: None,
            blocked_place_ids: Some(HashSet::from([10])),
        };
        assert!(broker.try_claim(&plugin(Some(10)), &blocked).is_none());

        let claim = broker.try_claim(&plugin(Some(10)), &no_filter()).unwrap();
        assert_eq!(claim.request_id, id);
    }

    #[test]
    fn serve_place_ids_filter() {
        let broker = ConnectBroker::new();
        created(broker.create(ConnectRequest::default()));

        let filter = PlaceFilter {
            serve_place_ids: Some(HashSet::from([1, 2])),
            blocked_place_ids: None,
        };

        assert!(broker.try_claim(&plugin(Some(3)), &filter).is_none());
        assert!(broker.try_claim(&plugin(Some(2)), &filter).is_some());
    }

    #[test]
    fn already_connected() {
        let broker = Arc::new(ConnectBroker::new());

        let guard = broker.track_socket();
        assert!(matches!(
            broker.create(ConnectRequest::default()),
            CreateOutcome::AlreadyConnected
        ));

        drop(guard);
        assert!(matches!(
            broker.create(ConnectRequest::default()),
            CreateOutcome::Created(_)
        ));
    }

    #[test]
    fn confirmation_decision() {
        let broker = ConnectBroker::new();
        let id = created(broker.create(ConnectRequest::default()));
        broker.try_claim(&plugin(None), &no_filter()).unwrap();

        // There's nothing to decide on until the plugin asks for confirmation.
        assert_eq!(broker.decide(id, ConnectDecision::Accept), Some(false));

        assert!(broker
            .update(id, update(1, ConnectState::Confirming))
            .is_ok());
        let (_, waiting) = broker.check_decision(id).unwrap();
        assert!(waiting);

        assert_eq!(broker.decide(id, ConnectDecision::Abort), Some(true));
        // The first decision wins.
        assert_eq!(broker.decide(id, ConnectDecision::Accept), Some(false));

        let (response, waiting) = broker.check_decision(id).unwrap();
        assert!(!waiting);
        assert_eq!(response.decision, Some(ConnectDecision::Abort));
    }

    #[test]
    fn stale_updates_are_ignored() {
        let broker = ConnectBroker::new();
        let id = created(broker.create(ConnectRequest::default()));
        broker.try_claim(&plugin(None), &no_filter()).unwrap();

        assert!(broker
            .update(id, update(2, ConnectState::Confirming))
            .is_ok());
        assert!(broker
            .update(id, update(1, ConnectState::Connecting))
            .is_ok());
        assert_eq!(broker.status(id).unwrap().state, ConnectState::Confirming);

        assert!(broker.update(id, update(3, ConnectState::Failed)).is_ok());
        assert!(broker
            .update(id, update(4, ConnectState::Connecting))
            .is_ok());
        assert_eq!(broker.status(id).unwrap().state, ConnectState::Failed);

        assert!(matches!(
            broker.update(id, update(5, ConnectState::Pending)),
            Err(UpdateError::InvalidState(ConnectState::Pending))
        ));
    }

    #[test]
    fn cancel_pending() {
        let broker = ConnectBroker::new();
        let id = created(broker.create(ConnectRequest::default()));

        assert_eq!(broker.cancel(id).unwrap().state, ConnectState::Cancelled);
        assert!(broker.try_claim(&plugin(None), &no_filter()).is_none());
    }

    #[test]
    fn abandoned_requests_are_not_claimed() {
        let broker = ConnectBroker::new();
        let id = created(broker.create(ConnectRequest::default()));

        broker.lock().get_mut(&id).unwrap().last_cli_activity =
            Instant::now() - CLI_ABANDON_TIMEOUT - Duration::from_secs(1);

        assert!(broker.try_claim(&plugin(None), &no_filter()).is_none());
    }

    #[tokio::test]
    async fn poll_wakes_for_new_requests() {
        let broker = Arc::new(ConnectBroker::new());

        let poller = {
            let broker = Arc::clone(&broker);
            tokio::spawn(async move { broker.poll(plugin(None), no_filter()).await })
        };

        tokio::time::sleep(Duration::from_millis(50)).await;
        let id = created(broker.create(ConnectRequest::default()));

        let claim = tokio::time::timeout(Duration::from_secs(5), poller)
            .await
            .expect("poll should wake up for the new request")
            .unwrap()
            .expect("poll should claim the new request");
        assert_eq!(claim.request_id, id);
    }

    #[test]
    fn local_peers() {
        let addr = |ip: IpAddr| SocketAddr::new(ip, 34872);
        let loopback = addr(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let lan = addr(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 5)));
        let other = addr(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 6)));
        let mapped = addr(IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()));
        let v6_loopback = addr(IpAddr::V6(Ipv6Addr::LOCALHOST));

        assert!(is_local_peer(loopback, loopback));
        assert!(is_local_peer(mapped, loopback));
        assert!(is_local_peer(v6_loopback, v6_loopback));
        assert!(is_local_peer(lan, lan));
        assert!(!is_local_peer(other, lan));
    }
}
