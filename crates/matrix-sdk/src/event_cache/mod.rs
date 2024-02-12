// Copyright 2024 The Matrix.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The event cache is an abstraction layer, sitting between the Rust SDK and a
//! final client, that acts as a global observer of all the rooms, gathering and
//! inferring some extra useful information about each room. In particular, this
//! doesn't require subscribing to a specific room to get access to this
//! information.
//!
//! It's intended to be fast, robust and easy to maintain.
//!
//! See the [github issue](https://github.com/matrix-org/matrix-rust-sdk/issues/3058) for more details about the historical reasons that led us to start writing this.
//!
//! Most of it is still a work-in-progress, as of 2024-01-22.
//!
//! The desired set of features it may eventually implement is the following:
//!
//! - [ ] compute proper unread room counts, and use backpagination to get
//!   missing messages/notifications/mentions, if needs be.
//! - [ ] expose that information with a new data structure similar to the
//!   `RoomInfo`, and that may update a `RoomListService`.
//! - [ ] provide read receipts for each message.
//! - [ ] backwards and forward pagination, and reconcile results with cached
//!   timelines.
//! - [ ] retry decryption upon receiving new keys (from an encryption sync
//!   service or from a key backup).
//! - [ ] expose the latest event for a given room.
//! - [ ] caching of events on-disk.

#![forbid(missing_docs)]

use std::{
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, OnceLock, Weak},
    time::{Duration, Instant},
};

use matrix_sdk_base::{
    deserialized_responses::{AmbiguityChange, SyncTimelineEvent},
    sync::{JoinedRoomUpdate, LeftRoomUpdate, RoomUpdates, Timeline},
};
use matrix_sdk_common::executor::{spawn, JoinHandle};
use ruma::{
    assign,
    events::{AnyRoomAccountDataEvent, AnySyncEphemeralRoomEvent},
    serde::Raw,
    OwnedEventId, OwnedRoomId, RoomId,
};
use tokio::{
    sync::{
        broadcast::{error::RecvError, Receiver, Sender},
        Mutex, RwLock,
    },
    time::sleep,
};
use tracing::{debug, error, trace, warn};

use self::store::{EventCacheStore, MemoryStore};
use crate::{
    client::ClientInner, event_cache::store::PaginationToken, room::MessagesOptions, Client, Room,
};

mod store;

/// An error observed in the [`EventCache`].
#[derive(thiserror::Error, Debug)]
pub enum EventCacheError {
    /// The [`EventCache`] instance hasn't been initialized with
    /// [`EventCache::subscribe`]
    #[error(
        "The EventCache hasn't subscribed to sync responses yet, call `EventCache::subscribe()`"
    )]
    NotSubscribedYet,

    /// The room hasn't been found in the client.
    ///
    /// Technically, it's possible to request a `RoomEventCache` for a room that
    /// is not known to the client, leading to this error.
    #[error("Room {0} hasn't been found in the Client.")]
    RoomNotFound(OwnedRoomId),

    /// The [`EventCache`] owns a weak reference to the [`Client`] it pertains
    /// to. It's possible this weak reference points to nothing anymore, at
    /// times where we try to use the client.
    #[error("The owning client of the event cache has been dropped.")]
    ClientDropped,

    /// Another error caused by the SDK happened somewhere, and we report it to
    /// the caller.
    #[error("SDK error: {0}")]
    SdkError(#[source] crate::Error),
}

/// A result using the [`EventCacheError`].
pub type Result<T> = std::result::Result<T, EventCacheError>;

/// Hold handles to the tasks spawn by a [`RoomEventCache`].
pub struct EventCacheDropHandles {
    listen_updates_task: JoinHandle<()>,
}

impl Debug for EventCacheDropHandles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventCacheDropHandles").finish_non_exhaustive()
    }
}

impl Drop for EventCacheDropHandles {
    fn drop(&mut self) {
        self.listen_updates_task.abort();
    }
}

/// An event cache, providing lots of useful functionality for clients.
///
/// Cloning is shallow, and thus is cheap to do.
///
/// See also the module-level comment.
#[derive(Clone)]
pub struct EventCache {
    /// Reference to the inner cache.
    inner: Arc<EventCacheInner>,
}

impl Debug for EventCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventCache").finish_non_exhaustive()
    }
}

impl EventCache {
    /// Create a new [`EventCache`] for the given client.
    pub(crate) fn new(client: &Arc<ClientInner>) -> Self {
        let store = Arc::new(MemoryStore::new());
        let inner = Arc::new(EventCacheInner {
            client: Arc::downgrade(client),
            by_room: Default::default(),
            store,
            process_lock: Default::default(),
            drop_handles: Default::default(),
        });

        Self { inner }
    }

    /// Starts subscribing the [`EventCache`] to sync responses, if not done
    /// before.
    ///
    /// Re-running this has no effect if we already subscribed before, and is
    /// cheap.
    pub fn subscribe(&self) -> Result<()> {
        let client = self.inner.client()?;

        let _ = self.inner.drop_handles.get_or_init(|| {
            // Spawn the task that will listen to all the room updates at once.
            let room_updates_feed = client.subscribe_to_all_room_updates();
            let listen_updates_task =
                spawn(Self::listen_task(self.inner.clone(), room_updates_feed));

            Arc::new(EventCacheDropHandles { listen_updates_task })
        });

        Ok(())
    }

    async fn listen_task(
        inner: Arc<EventCacheInner>,
        mut room_updates_feed: Receiver<RoomUpdates>,
    ) {
        trace!("Spawning the listen task");
        loop {
            match room_updates_feed.recv().await {
                Ok(updates) => {
                    if let Err(err) = inner.handle_room_updates(updates).await {
                        match err {
                            EventCacheError::ClientDropped => {
                                // The client has dropped, exit the listen task.
                                break;
                            }
                            err => {
                                error!("Error when handling room updates: {err}");
                            }
                        }
                    }
                }

                Err(RecvError::Lagged(_)) => {
                    // Forget everything we know; we could have missed events, and we have
                    // no way to reconcile at the moment!
                    // TODO: implement Smart Matching™,
                    let mut by_room = inner.by_room.write().await;
                    for room_id in by_room.keys() {
                        if let Err(err) = inner.store.clear_room(room_id).await {
                            error!("unable to clear room after room updates lag: {err}");
                        }
                    }
                    by_room.clear();
                }

                Err(RecvError::Closed) => {
                    // The sender has shut down, exit.
                    break;
                }
            }
        }
    }

    /// Return a room-specific view over the [`EventCache`].
    pub async fn for_room(
        &self,
        room_id: &RoomId,
    ) -> Result<(RoomEventCache, Arc<EventCacheDropHandles>)> {
        let Some(drop_handles) = self.inner.drop_handles.get().cloned() else {
            return Err(EventCacheError::NotSubscribedYet);
        };

        let room = self.inner.for_room(room_id).await?;

        Ok((room, drop_handles))
    }

    /// Add an initial set of events to the event cache, reloaded from a cache.
    ///
    /// TODO: temporary for API compat, as the event cache should take care of
    /// its own store.
    pub async fn add_initial_events(
        &self,
        room_id: &RoomId,
        events: Vec<SyncTimelineEvent>,
        prev_batch: Option<String>,
    ) -> Result<()> {
        let room_cache = self.inner.for_room(room_id).await?;

        // We could have received events during a previous sync; remove them all, since
        // we can't know where to insert the "initial events" with respect to
        // them.
        self.inner.store.clear_room(room_id).await?;
        let _ = room_cache.inner.sender.send(RoomEventCacheUpdate::Clear);

        room_cache
            .inner
            .append_events(
                events,
                prev_batch,
                Default::default(),
                Default::default(),
                Default::default(),
            )
            .await?;

        Ok(())
    }
}

struct EventCacheInner {
    /// A weak reference to the inner client, useful when trying to get a handle
    /// on the owning client.
    client: Weak<ClientInner>,

    /// Lazily-filled cache of live [`RoomEventCache`], once per room.
    by_room: RwLock<BTreeMap<OwnedRoomId, RoomEventCache>>,

    /// Backend used for storage.
    store: Arc<dyn EventCacheStore>,

    /// A lock to make sure that despite multiple updates coming to the
    /// `EventCache`, it will only handle one at a time.
    ///
    /// [`Mutex`] is “fair”, as it is implemented as a FIFO. It is important to
    /// ensure that multiple updates will be applied in the correct order.
    process_lock: Mutex<()>,

    /// Handles to keep alive the task listening to updates.
    drop_handles: OnceLock<Arc<EventCacheDropHandles>>,
}

impl EventCacheInner {
    fn client(&self) -> Result<Client> {
        Ok(Client { inner: self.client.upgrade().ok_or(EventCacheError::ClientDropped)? })
    }

    /// Handles a single set of room updates at once.
    async fn handle_room_updates(&self, updates: RoomUpdates) -> Result<()> {
        // First, take the lock that indicates we're processing updates, to avoid
        // handling multiple updates concurrently.
        let _process_lock = self.process_lock.lock().await;

        // Left rooms.
        for (room_id, left_room_update) in updates.leave {
            let room = self.for_room(&room_id).await?;

            if let Err(err) = room.inner.handle_left_room_update(left_room_update).await {
                // Non-fatal error, try to continue to the next room.
                error!("handling left room update: {err}");
            }
        }

        // Joined rooms.
        for (room_id, joined_room_update) in updates.join {
            let room = self.for_room(&room_id).await?;

            if let Err(err) = room.inner.handle_joined_room_update(joined_room_update).await {
                // Non-fatal error, try to continue to the next room.
                error!("handling joined room update: {err}");
            }
        }

        // Invited rooms.
        // TODO: we don't anything with `updates.invite` at this point.

        Ok(())
    }

    /// Return a room-specific view over the [`EventCache`].
    ///
    /// It may not be found, if the room isn't known to the client.
    async fn for_room(&self, room_id: &RoomId) -> Result<RoomEventCache> {
        // Fast path: the entry exists; let's acquire a read lock, it's cheaper than a
        // write lock.
        let by_room_guard = self.by_room.read().await;

        match by_room_guard.get(room_id) {
            Some(room) => Ok(room.clone()),

            None => {
                // Slow-path: the entry doesn't exist; let's acquire a write lock.
                drop(by_room_guard);
                let mut by_room_guard = self.by_room.write().await;

                // In the meanwhile, some other caller might have obtained write access and done
                // the same, so check for existence again.
                if let Some(room) = by_room_guard.get(room_id) {
                    return Ok(room.clone());
                }

                let room = self
                    .client()?
                    .get_room(room_id)
                    .ok_or_else(|| EventCacheError::RoomNotFound(room_id.to_owned()))?;

                let room_event_cache = RoomEventCache::new(room, self.store.clone());

                by_room_guard.insert(room_id.to_owned(), room_event_cache.clone());

                Ok(room_event_cache)
            }
        }
    }
}

/// A subset of an event cache, for a room.
///
/// Cloning is shallow, and thus is cheap to do.
#[derive(Clone)]
pub struct RoomEventCache {
    inner: Arc<RoomEventCacheInner>,
}

impl Debug for RoomEventCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoomEventCache").finish_non_exhaustive()
    }
}

impl RoomEventCache {
    /// Create a new [`RoomEventCache`] using the given room and store.
    fn new(room: Room, store: Arc<dyn EventCacheStore>) -> Self {
        Self { inner: Arc::new(RoomEventCacheInner::new(room, store)) }
    }

    /// Subscribe to room updates for this room, after getting the initial list
    /// of events. XXX: Could/should it use some kind of `Observable`
    /// instead? Or not something async, like explicit handlers as our event
    /// handlers?
    pub async fn subscribe(
        &self,
    ) -> Result<(Vec<SyncTimelineEvent>, Receiver<RoomEventCacheUpdate>)> {
        Ok((
            self.inner.store.room_events(self.inner.room.room_id()).await?,
            self.inner.sender.subscribe(),
        ))
    }

    /// Back-paginate until we hit the stop condition.
    pub async fn backpaginate_until<F: Fn(usize) -> bool>(
        &self,
        should_continue_func: F,
    ) -> Result<BackpaginationResult> {
        self.inner.backpaginate_until(should_continue_func).await
    }

    /// Wait for a backpagination token received from sync, for this room.
    ///
    /// This function can return before the `max_wait` duration happened.
    ///
    /// Returns whether we obtained one after waiting.
    pub async fn wait_for_backpagination_token(&self, max_wait: Duration) -> Result<bool> {
        self.inner.wait_for_backpagination_token(max_wait).await
    }
}

/// The (non-clonable) details of the `RoomEventCache`.
struct RoomEventCacheInner {
    /// Sender part for subscribers to this room.
    sender: Sender<RoomEventCacheUpdate>,

    /// A pointer to the store implementation used for this event cache.
    store: Arc<dyn EventCacheStore>,

    /// The Client [`Room`] this event cache pertains to.
    room: Room,

    /// A lock that ensures we don't run multiple pagination queries at the same
    /// time.
    pagination_lock: Mutex<()>,
}

impl RoomEventCacheInner {
    /// Creates a new cache for a room, and subscribes to room updates, so as
    /// to handle new timeline events.
    fn new(room: Room, store: Arc<dyn EventCacheStore>) -> Self {
        let sender = Sender::new(32);
        Self { room, store, sender, pagination_lock: Default::default() }
    }

    async fn handle_joined_room_update(&self, updates: JoinedRoomUpdate) -> Result<()> {
        self.handle_timeline(
            updates.timeline,
            updates.ephemeral.clone(),
            updates.account_data,
            updates.ambiguity_changes,
        )
        .await?;
        Ok(())
    }

    async fn handle_timeline(
        &self,
        timeline: Timeline,
        ephemeral: Vec<Raw<AnySyncEphemeralRoomEvent>>,
        account_data: Vec<Raw<AnyRoomAccountDataEvent>>,
        ambiguity_changes: BTreeMap<OwnedEventId, AmbiguityChange>,
    ) -> Result<()> {
        if timeline.limited {
            // Ideally we'd try to reconcile existing events against those received in the
            // timeline, but we're not there yet. In the meanwhile, clear the
            // items from the room. TODO: implement Smart Matching™.
            trace!("limited timeline, clearing all previous events");

            // Clear internal state (events, pagination tokens, etc.).
            self.store.clear_room(self.room.room_id()).await?;

            // Propagate to observers.
            let _ = self.sender.send(RoomEventCacheUpdate::Clear);
        }

        // Add all the events to the backend.
        trace!("adding new events");
        self.append_events(
            timeline.events,
            timeline.prev_batch,
            account_data,
            ephemeral,
            ambiguity_changes,
        )
        .await?;

        Ok(())
    }

    async fn handle_left_room_update(&self, updates: LeftRoomUpdate) -> Result<()> {
        self.handle_timeline(updates.timeline, Vec::new(), Vec::new(), updates.ambiguity_changes)
            .await?;
        Ok(())
    }

    /// Append a set of events to the room cache and storage, notifying
    /// observers.
    async fn append_events(
        &self,
        events: Vec<SyncTimelineEvent>,
        prev_batch: Option<String>,
        account_data: Vec<Raw<AnyRoomAccountDataEvent>>,
        ephemeral: Vec<Raw<AnySyncEphemeralRoomEvent>>,
        ambiguity_changes: BTreeMap<OwnedEventId, AmbiguityChange>,
    ) -> Result<()> {
        if events.is_empty()
            && prev_batch.is_none()
            && ephemeral.is_empty()
            && account_data.is_empty()
            && ambiguity_changes.is_empty()
        {
            return Ok(());
        }

        let room_id = self.room.room_id();
        self.store.add_room_events(room_id, events.clone()).await?;

        // Events are appended to the end of the events array, so push the token to the
        // end too.
        if let Some(prev_batch) = prev_batch.clone() {
            self.push_backpagination_token(prev_batch, &events).await?;
        }

        let _ = self.sender.send(RoomEventCacheUpdate::Append {
            events,
            prev_batch,
            account_data,
            ephemeral,
            ambiguity_changes,
        });

        Ok(())
    }

    // Methods related to backpagination
    // TODO: move elsewhere?

    /// Appends a backpagination token to the list, after new items have been
    /// appended to the room.
    ///
    /// It is the caller's responsibility to notify about a change here.
    async fn push_backpagination_token(
        &self,
        token: String,
        events: &[SyncTimelineEvent],
    ) -> Result<()> {
        // Find an appropriate event to attach the pagination token to, starting from
        // the start.
        //
        // TODO: (bnjbvr) it would be more precise to keep the current event position
        // (=len()) and a generation counter, then resort to event id only if
        // when inserting the backpaginated events, the generation changed.
        let mut added = false;

        for event in events {
            if let Some(event_id) = event.event_id() {
                trace!(token, ?event_id, "Adding back-pagination token to the back");
                let token = PaginationToken { event_id, token };
                self.store.push_backpagination_token(self.room.room_id(), token).await?;
                added = true;
                break;
            }
        }

        if !added {
            warn!(
                "Back-pagination token couldn't be attached, because there's no event with an id"
            );
        }

        Ok(())
    }

    /// Run a single back-pagination `/messages` request.
    ///
    /// This will only run one request; since a backpagination may need to
    /// continue, it's preferable to use [`Self::backpaginate_until`].
    ///
    /// Returns the number of messages received in this chunk.
    async fn backpaginate_once(
        &self,
        token: PaginationToken,
        requested_num_events: u16,
    ) -> Result<usize> {
        let messages = self
            .room
            .messages(assign!(MessagesOptions::backward(), {
                from: Some(token.token.clone()),
                limit: requested_num_events.into()
            }))
            .await
            .map_err(EventCacheError::SdkError)?;

        let num_events = messages.chunk.len();

        // The chunk could be empty. If there's any event, they are presented in reverse
        // order (i.e. the first one should be prepended first).
        //
        // Would we want to backpaginate again, we'd start from the `end` token as the
        // next `from` token. As a matter of fact, if a back-pagination token is
        // present, then it should be added to the last event that has an event
        // id.
        if let Some(prev_token) = messages.end {
            let mut target_event = None;

            for event in messages.chunk.iter().rev() {
                if let Some(event_id) = event.event.get_field("event_id").ok().flatten() {
                    target_event = Some(event_id);
                    break;
                }
            }

            // If there wasn't any event with an id, attach the backpagination token to the
            // same event that caused this backpagination.
            let event_id = target_event.unwrap_or_else(|| {
                debug!("no event found for a prepended backpagination token; reusing previous");
                token.event_id
            });

            let new_token = PaginationToken { event_id: event_id.to_owned(), token: prev_token };
            debug!(%event_id, token = new_token.token, "prepending a backpagination token");

            // Save it in the store.

            // TODO: what if we received another backpagination token from sync? That would
            // get messy...
            // Likely: take the process lock! (or put it in the store? or have store
            // transactions?)
            self.store.prepend_backpagination_token(self.room.room_id(), new_token).await?;
        }

        // Prepend the events, in inverse order.
        //
        // It's fine to convert from `TimelineEvent` (i.e. that has a room id) to
        // `SyncTimelineEvent` (i.e. that doesn't have it), because those events are
        // always tied to a room in storage anyways.
        self.store
            .prepend_room_events(
                self.room.room_id(),
                messages.chunk.into_iter().rev().map(Into::into).collect(),
            )
            .await?;

        Ok(num_events)
    }

    /// Wait for a backpagination token received from sync, for this room.
    ///
    /// This function can return before the `max_wait` duration happened.
    ///
    /// Returns whether we obtained one after waiting.
    async fn wait_for_backpagination_token(&self, max_wait: Duration) -> Result<bool> {
        // TODO should we instead use a sender from the parent event cache to here?

        // Exponential backoff, factor 2, starting from 100ms, up to max_wait.
        let start_time = Instant::now();
        let mut wait_time = Duration::from_millis(100).min(max_wait);
        loop {
            if self.store.earliest_backpagination_token(self.room.room_id()).await?.is_some() {
                return Ok(true);
            }

            sleep(wait_time).await;
            wait_time *= 2;

            if Instant::now() - start_time > max_wait {
                return Ok(false);
            }
        }
    }

    /// Back-paginate until we hit the stop condition.
    async fn backpaginate_until<F: Fn(usize) -> bool>(
        &self,
        should_continue_func: F,
    ) -> Result<BackpaginationResult> {
        // First, take the lock to make sure we don't have have multiple pagination
        // requests at the same time.
        let _lock = self.pagination_lock.lock().await;

        const BACKPAGINATION_BATCH_SIZE: u16 = 20;

        let mut num_events = 0;
        let mut num_iter = 0;
        let mut reached_start = false;

        // There are two ways to escape this loop:
        // - either we hit the number of total requested events,
        // - or there wasn't any new events to get.
        loop {
            if let Some(token) =
                self.store.earliest_backpagination_token(self.room.room_id()).await?
            {
                let num_new_events =
                    self.backpaginate_once(token.clone(), BACKPAGINATION_BATCH_SIZE).await?;

                num_events += num_new_events;
                num_iter += 1;

                // Now that the backpagination request has been successful, remove the previous
                // token.
                self.store.remove_backpagination_token(self.room.room_id(), &token).await?;

                if !should_continue_func(num_events) {
                    break;
                }
            } else {
                // No more token: we reached the start of the room!
                reached_start = true;
                break;
            }
        }

        Ok(BackpaginationResult { reached_start, num_iterations: num_iter, num_events })
    }
}

/// The result of a back-pagination query.
#[derive(Debug)]
pub struct BackpaginationResult {
    /// Did we reach the timeline start?
    pub reached_start: bool,
    /// Number of back-pagination iterations.
    pub num_iterations: usize,
    /// Number of new events that have been added.
    pub num_events: usize,
}

/// An update related to events happened in a room.
#[derive(Debug, Clone)]
pub enum RoomEventCacheUpdate {
    /// The room has been cleared from events.
    Clear,
    /// The room has new events that must be prepended.
    Prepend {
        /// Events that should be prepended to the room's timeline.
        events: Vec<SyncTimelineEvent>,
    },
    /// The room has new events.
    Append {
        /// All the new events that have been added to the room's timeline.
        events: Vec<SyncTimelineEvent>,
        /// XXX: this is temporary, until prev_batch lives in the event cache
        prev_batch: Option<String>,
        /// XXX: this is temporary, until account data lives in the event cache
        /// — or will it live there?
        account_data: Vec<Raw<AnyRoomAccountDataEvent>>,
        /// XXX: this is temporary, until read receipts are handled in the event
        /// cache
        ephemeral: Vec<Raw<AnySyncEphemeralRoomEvent>>,
        /// Collection of ambiguity changes that room member events trigger.
        ///
        /// This is a map of event ID of the `m.room.member` event to the
        /// details of the ambiguity change.
        ambiguity_changes: BTreeMap<OwnedEventId, AmbiguityChange>,
    },
}
