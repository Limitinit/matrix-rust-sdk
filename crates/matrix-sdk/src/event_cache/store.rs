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

use std::collections::{BTreeMap, VecDeque};

use async_trait::async_trait;
use matrix_sdk_common::deserialized_responses::SyncTimelineEvent;
use ruma::{OwnedEventId, OwnedRoomId, RoomId};
use tokio::sync::RwLock;

use super::Result;

/// A store that can be remember information about the event cache.
///
/// It really acts as a cache, in the sense that clearing the backing data
/// should not have any irremediable effect, other than providing a lesser user
/// experience.
#[async_trait]
pub trait EventCacheStore: Send + Sync {
    /// Returns all the known events for the given room.
    async fn room_events(&self, room: &RoomId) -> Result<Vec<SyncTimelineEvent>>;

    /// Adds all the events to the given room's timeline.
    async fn add_room_events(&self, room: &RoomId, events: Vec<SyncTimelineEvent>) -> Result<()>;

    /// Prepends all the events to the given room's timeline.
    async fn prepend_room_events(
        &self,
        room: &RoomId,
        events: Vec<SyncTimelineEvent>,
    ) -> Result<()>;

    async fn remove_backpagination_token(
        &self,
        room: &RoomId,
        token: &PaginationToken,
    ) -> Result<()>;

    /// Retrieve the earliest (oldest backpagination token for the given room.)
    async fn earliest_backpagination_token(&self, room: &RoomId)
        -> Result<Option<PaginationToken>>;

    /// Push the given back-pagination token to the room's list.
    async fn push_backpagination_token(&self, room: &RoomId, token: PaginationToken) -> Result<()>;

    /// Prepend the given back-pagination token to the room's list.
    async fn prepend_backpagination_token(
        &self,
        room: &RoomId,
        token: PaginationToken,
    ) -> Result<()>;

    /// Clear all the information tied to a given room.
    ///
    /// This forgets the following:
    /// - events in the room
    /// - pagination tokens
    async fn clear_room(&self, room: &RoomId) -> Result<()>;
}

/// A pagination token.
#[derive(Clone, PartialEq)]
pub struct PaginationToken {
    /// The event to which we should attach the result of the backpagination.
    pub event_id: OwnedEventId,

    /// The token to use in the query, extracted from a previous "from" / "end"
    /// field of a `/messages` response.
    pub token: String,
}

/// All the information related to a room and stored in the event cache.
#[derive(Default)]
struct RoomInfo {
    /// All the events per room, in sync order.
    events: Vec<SyncTimelineEvent>,

    /// Backpagination tokens, ordered by the sync order of the referenced
    /// event.
    backpagination_tokens: VecDeque<PaginationToken>,
}

impl RoomInfo {
    fn clear(&mut self) {
        self.events.clear();
        self.backpagination_tokens.clear();
    }
}

/// An [`EventCacheStore`] implementation that keeps all the information in
/// memory.
#[derive(Default)]
pub(crate) struct MemoryStore {
    by_room: RwLock<BTreeMap<OwnedRoomId, RoomInfo>>,
}

impl MemoryStore {
    /// Create a new empty [`MemoryStore`].
    pub fn new() -> Self {
        Default::default()
    }
}

#[async_trait]
impl EventCacheStore for MemoryStore {
    async fn room_events(&self, room: &RoomId) -> Result<Vec<SyncTimelineEvent>> {
        Ok(self
            .by_room
            .read()
            .await
            .get(room)
            .map(|room_info| &room_info.events)
            .cloned()
            .unwrap_or_default())
    }

    async fn add_room_events(&self, room: &RoomId, events: Vec<SyncTimelineEvent>) -> Result<()> {
        self.by_room.write().await.entry(room.to_owned()).or_default().events.extend(events);
        Ok(())
    }

    async fn prepend_room_events(
        &self,
        room: &RoomId,
        events: Vec<SyncTimelineEvent>,
    ) -> Result<()> {
        self.by_room.write().await.entry(room.to_owned()).or_default().events.splice(0..0, events);
        Ok(())
    }

    async fn clear_room(&self, room: &RoomId) -> Result<()> {
        // Clear the room, so as to avoid reallocations if the room is being reused.
        // XXX: do we also want an actual way to *remove* a room? (for left rooms)
        if let Some(room) = self.by_room.write().await.get_mut(room) {
            room.clear();
        }
        Ok(())
    }

    async fn push_backpagination_token(&self, room: &RoomId, token: PaginationToken) -> Result<()> {
        self.by_room
            .write()
            .await
            .entry(room.to_owned())
            .or_default()
            .backpagination_tokens
            .push_back(token);
        Ok(())
    }

    async fn prepend_backpagination_token(
        &self,
        room: &RoomId,
        token: PaginationToken,
    ) -> Result<()> {
        self.by_room
            .write()
            .await
            .entry(room.to_owned())
            .or_default()
            .backpagination_tokens
            .push_front(token);
        Ok(())
    }

    async fn earliest_backpagination_token(
        &self,
        room: &RoomId,
    ) -> Result<Option<PaginationToken>> {
        Ok(self
            .by_room
            .read()
            .await
            .get(room)
            .and_then(|room| room.backpagination_tokens.front().cloned()))
    }

    async fn remove_backpagination_token(
        &self,
        room: &RoomId,
        token: &PaginationToken,
    ) -> Result<()> {
        let mut by_room_guard = self.by_room.write().await;
        let room = by_room_guard.entry(room.to_owned()).or_default();
        if let Some(pos) = room
            .backpagination_tokens
            .iter()
            .enumerate()
            .find_map(|(i, t)| (t == token).then_some(i))
        {
            room.backpagination_tokens.remove(pos);
        }
        Ok(())
    }
}
