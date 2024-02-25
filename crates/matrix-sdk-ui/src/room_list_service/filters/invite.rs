use matrix_sdk::{Client, RoomListEntry};
use matrix_sdk_base::RoomState;

use super::Filter;

struct InviteRoomMatcher<F>
where
    F: Fn(&RoomListEntry) -> Option<bool>,
{
    is_invite: F,
}

impl<F> InviteRoomMatcher<F>
where
    F: Fn(&RoomListEntry) -> Option<bool>,
{
    fn matches(&self, room_list_entry: &RoomListEntry) -> bool {
        if !matches!(room_list_entry, RoomListEntry::Filled(_) | RoomListEntry::Invalidated(_)) {
            return false;
        }

        (self.is_invite)(room_list_entry).unwrap_or(false)
    }
}

/// Create a new filter that will accept all filled or invalidated entries, but
/// filters out rooms that are not marked as favourite (see
/// [`matrix_sdk_base::Room::is_favourite`]).
pub fn new_filter(client: &Client) -> impl Filter {
    let client = client.clone();

    let matcher = InviteRoomMatcher {
        is_invite: move |room| match room {
            RoomListEntry::Filled(room_id) | RoomListEntry::Invalidated(room_id) => {
                let room = client.get_room(room_id)?;
                if room.state() == RoomState::Invited {
                    Some(true)
                } else {
                    Some(false)
                }
            }
            _ => None,
        },
    };

    move |room_list_entry| -> bool { matcher.matches(room_list_entry) }
}
