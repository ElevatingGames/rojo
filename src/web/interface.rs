//! Defines all the structs needed to interact with the Rojo Serve API. This is
//! useful for tests to be able to use the same data structures as the
//! implementation.

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

use rbx_dom_weak::{
    types::{Ref, Variant, VariantType},
    Ustr, UstrMap,
};
use serde::{Deserialize, Serialize};
use strum::Display;

use crate::{
    session_id::SessionId,
    snapshot::{
        AppliedPatchSet, InstanceMetadata as RojoInstanceMetadata, InstanceWithMeta, RojoTree,
    },
};

/// Server version to report over the API, not exposed outside this crate.
pub(crate) const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Current protocol version, which is required to match.
pub const PROTOCOL_VERSION: u64 = 5;

/// Message returned by Rojo API when a change has occurred.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeMessage<'a> {
    pub removed: Vec<Ref>,
    pub added: HashMap<Ref, Instance<'a>>,
    pub updated: Vec<InstanceUpdate>,
}

impl<'a> SubscribeMessage<'a> {
    pub(crate) fn from_patch_update(tree: &'a RojoTree, patch: AppliedPatchSet) -> Self {
        let removed = patch.removed;

        let mut added = HashMap::new();
        for id in patch.added {
            let instance = tree.get_instance(id).unwrap();
            added.insert(id, Instance::from_rojo_instance(instance));

            for instance in tree.descendants(id) {
                added.insert(instance.id(), Instance::from_rojo_instance(instance));
            }
        }

        let updated = patch
            .updated
            .into_iter()
            .map(|update| {
                let changed_metadata = update
                    .changed_metadata
                    .as_ref()
                    .map(InstanceMetadata::from_rojo_metadata);

                let changed_properties = update
                    .changed_properties
                    .into_iter()
                    .filter(|(_key, value)| property_filter(value.as_ref()))
                    .collect();

                InstanceUpdate {
                    id: update.id,
                    changed_name: update.changed_name,
                    changed_class_name: update.changed_class_name,
                    changed_properties,
                    changed_metadata,
                }
            })
            .collect();

        Self {
            removed,
            added,
            updated,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceUpdate {
    pub id: Ref,
    pub changed_name: Option<String>,
    pub changed_class_name: Option<Ustr>,

    // TODO: Transform from UstrMap<String, Option<_>> to something else, since
    // null will get lost when decoding from JSON in some languages.
    #[serde(default)]
    pub changed_properties: UstrMap<Option<Variant>>,
    pub changed_metadata: Option<InstanceMetadata>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceMetadata {
    pub ignore_unknown_instances: bool,
}

impl InstanceMetadata {
    pub(crate) fn from_rojo_metadata(meta: &RojoInstanceMetadata) -> Self {
        Self {
            ignore_unknown_instances: meta.ignore_unknown_instances,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Instance<'a> {
    pub id: Ref,
    pub parent: Ref,
    pub name: Cow<'a, str>,
    pub class_name: Ustr,
    pub properties: UstrMap<Cow<'a, Variant>>,
    pub children: Cow<'a, [Ref]>,
    pub metadata: Option<InstanceMetadata>,
}

impl Instance<'_> {
    pub(crate) fn from_rojo_instance(source: InstanceWithMeta<'_>) -> Instance<'_> {
        let properties = source
            .properties()
            .iter()
            .filter(|(_key, value)| property_filter(Some(value)))
            .map(|(key, value)| (*key, Cow::Borrowed(value)))
            .collect();

        Instance {
            id: source.id(),
            parent: source.parent(),
            name: Cow::Borrowed(source.name()),
            class_name: source.class_name(),
            properties,
            children: Cow::Borrowed(source.children()),
            metadata: Some(InstanceMetadata::from_rojo_metadata(source.metadata())),
        }
    }
}

fn property_filter(value: Option<&Variant>) -> bool {
    let ty = value.map(|value| value.ty());

    // Lua can't do anything with SharedString values. They also can't be
    // serialized directly by Serde!
    ty != Some(VariantType::SharedString)
}

/// Response body from /api/rojo
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerInfoResponse {
    pub session_id: SessionId,
    pub server_version: String,
    pub protocol_version: u64,
    pub project_name: String,
    pub expected_place_ids: Option<HashSet<u64>>,
    pub unexpected_place_ids: Option<HashSet<u64>>,
    pub game_id: Option<u64>,
    pub place_id: Option<u64>,
    pub root_instance_id: Ref,
}

/// Response body from /api/read/{id}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadResponse<'a> {
    pub session_id: SessionId,
    pub message_cursor: u32,
    pub instances: HashMap<Ref, Instance<'a>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteRequest {
    pub session_id: SessionId,
    pub removed: Vec<Ref>,

    #[serde(default)]
    pub added: HashMap<Ref, ()>,
    pub updated: Vec<InstanceUpdate>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteResponse {
    pub session_id: SessionId,
}

/// Packet type enum for different websocket message types
#[derive(Debug, Serialize, Deserialize, Display, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "camelCase")]
pub enum SocketPacketType {
    Messages,
    // TODO: Can we cleanly use the socket for all communication?
    // Serialize,
    // RefPatch,
}

/// Body content for messages packet type
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessagesPacket<'a> {
    pub message_cursor: u32,
    pub messages: Vec<SubscribeMessage<'a>>,
}

/// Body content for different packet types
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SocketPacketBody<'a> {
    Messages(MessagesPacket<'a>),
    // TODO: Can we cleanly use the socket for all communication?
    // Serialize(SerializePacket),
    // RefPatch(RefPatchPacket<'a>),
}

/// Message content from /api/socket
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SocketPacket<'a> {
    pub session_id: SessionId,
    pub packet_type: SocketPacketType,
    pub body: SocketPacketBody<'a>,
}

/// Response body from /api/open/{id}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenResponse {
    pub session_id: SessionId,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializeRequest {
    pub session_id: SessionId,
    pub ids: Vec<Ref>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializeResponse {
    pub session_id: SessionId,
    #[serde(with = "serde_bytes")]
    pub model_contents: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RefPatchRequest {
    pub session_id: SessionId,
    pub ids: HashSet<Ref>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RefPatchResponse<'a> {
    pub session_id: SessionId,
    pub patch: SubscribeMessage<'a>,
}

/// Identifies a request, made by `rojo connect`, for the Studio plugin to
/// start a sync session.
pub type ConnectRequestId = uuid::Uuid;

/// Request body for `POST /api/connect`, sent by `rojo connect`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectRequest {
    /// Accept any changes to Studio without asking for confirmation.
    #[serde(default)]
    pub force: bool,

    /// Only let a Studio window with this place open pick up the request.
    #[serde(default)]
    pub place_id: Option<u64>,
}

/// Response body from `POST /api/connect`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectResponse {
    pub session_id: SessionId,
    pub project_name: String,

    /// True if a Studio plugin is already connected, in which case no request
    /// was created.
    pub already_connected: bool,

    /// The new request, present unless `already_connected` is true.
    pub request_id: Option<ConnectRequestId>,
}

/// Where a connect request is in its lifecycle.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Display, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "camelCase")]
pub enum ConnectState {
    /// Waiting for a Studio plugin to pick up the request.
    Pending,
    /// A plugin picked up the request and is connecting.
    Connecting,
    /// The plugin found changes it needs confirmation for before syncing.
    Confirming,
    /// The plugin connected. This is a final state.
    Connected,
    /// The connection was aborted, usually because changes were declined.
    /// This is a final state.
    Aborted,
    /// The plugin failed to connect. This is a final state.
    Failed,
    /// `rojo connect` withdrew the request before any plugin picked it up.
    /// This is a final state.
    Cancelled,
}

impl ConnectState {
    pub fn is_final(self) -> bool {
        matches!(
            self,
            Self::Connected | Self::Aborted | Self::Failed | Self::Cancelled
        )
    }
}

/// The CLI's answer to a confirmation, or the plugin's view of it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ConnectDecision {
    Accept,
    Abort,
}

/// Describes the changes a plugin would make to Studio when connecting, as
/// shown on its confirmation page.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeSummary {
    /// Number of instances that would be added to Studio, counting descendants.
    #[serde(default)]
    pub added: u32,
    /// Number of instances that would be removed from Studio, not counting
    /// their descendants.
    #[serde(default)]
    pub removed: u32,
    /// Number of instances that would have properties changed in Studio.
    #[serde(default)]
    pub updated: u32,

    /// Individual changes, possibly truncated. Added instances are only listed
    /// at the top of each added subtree.
    #[serde(default)]
    pub entries: Vec<ChangeEntry>,

    /// How many entries there were before truncation.
    #[serde(default)]
    pub total_entries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeEntry {
    pub kind: ChangeKind,
    /// Full name of the instance, like `ReplicatedStorage.Shared.Module`.
    pub path: String,
    #[serde(default)]
    pub class_name: String,
    /// For updates, the names of the properties that would change.
    #[serde(default)]
    pub properties: Vec<String>,
    /// For additions and removals, how many descendants come along with it.
    #[serde(default)]
    pub descendants: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ChangeKind {
    Add,
    Remove,
    Update,
}

/// Describes the Studio plugin that picked up a connect request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectPluginInfo {
    pub place_id: Option<u64>,
    pub place_name: Option<String>,
}

/// Response body from `GET /api/connect/{id}` and `POST /api/connect/{id}/cancel`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectStatusResponse {
    pub request_id: ConnectRequestId,
    pub state: ConnectState,

    /// Progress text while connecting, or the reason for an abort or failure.
    pub message: Option<String>,

    /// The changes awaiting confirmation, once the plugin has reported them.
    pub changes: Option<ChangeSummary>,

    /// The plugin that picked up the request, if any.
    pub plugin: Option<ConnectPluginInfo>,

    /// Whether the plugin that picked up the request has reported back since.
    pub plugin_acknowledged: bool,

    /// Seconds since the plugin last contacted the server about this request.
    pub secs_since_plugin_activity: Option<f64>,

    /// Place IDs of Studio windows that polled for requests while this one was
    /// pending, but were not allowed to pick it up (because of `--place-id`,
    /// `servePlaceIds`, or `blockedPlaceIds`).
    pub ineligible_place_ids: Vec<u64>,
}

/// Request body for `POST /api/connect/{id}/decision`, sent by `rojo connect`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectDecisionRequest {
    pub decision: ConnectDecision,
}

/// Response body from `POST /api/connect/{id}/decision`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectDecisionResponse {
    /// False if the request was no longer waiting for a decision, for example
    /// because it was already answered in Studio.
    pub applied: bool,
}

/// Request body for `POST /api/connect/poll`, sent by the plugin.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectPollRequest {
    /// The plugin's place ID. Sent as a string because the plugin's msgpack
    /// encoder turns integers above 2^32 into floats.
    pub place_id: Option<String>,
    pub place_name: Option<String>,
}

/// Response body from `POST /api/connect/poll`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectPollResponse {
    /// The request this plugin claimed, or `None` if the poll timed out.
    pub request: Option<ClaimedConnectRequest>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimedConnectRequest {
    pub request_id: ConnectRequestId,
    pub force: bool,
}

/// Request body for `POST /api/connect/{id}/status`, sent by the plugin.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectStatusUpdate {
    /// Increases with every update so that updates which arrive out of order
    /// can be ignored.
    pub seq: u32,
    pub state: ConnectState,
    pub message: Option<String>,
    pub changes: Option<ChangeSummary>,
}

/// Response body from `POST /api/connect/{id}/status`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectStatusUpdateResponse {}

/// Response body from `GET /api/connect/{id}/decision`, polled by the plugin.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectDecisionPollResponse {
    pub state: ConnectState,
    /// The CLI's decision, or `None` if the poll timed out without one.
    pub decision: Option<ConnectDecision>,
}

/// General response type returned from all Rojo routes
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    kind: ErrorResponseKind,
    details: String,
}

impl ErrorResponse {
    pub fn kind(&self) -> &ErrorResponseKind {
        &self.kind
    }

    pub fn details(&self) -> &str {
        &self.details
    }

    pub fn not_found<S: Into<String>>(details: S) -> Self {
        Self {
            kind: ErrorResponseKind::NotFound,
            details: details.into(),
        }
    }

    pub fn bad_request<S: Into<String>>(details: S) -> Self {
        Self {
            kind: ErrorResponseKind::BadRequest,
            details: details.into(),
        }
    }

    pub fn forbidden<S: Into<String>>(details: S) -> Self {
        Self {
            kind: ErrorResponseKind::Forbidden,
            details: details.into(),
        }
    }

    pub fn internal_error<S: Into<String>>(details: S) -> Self {
        Self {
            kind: ErrorResponseKind::InternalError,
            details: details.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ErrorResponseKind {
    NotFound,
    BadRequest,
    Forbidden,
    InternalError,
}
