//! High-level push notification settings API

use std::sync::Arc;

use ruma::{
    api::client::push::{delete_pushrule, set_pushrule, set_pushrule_enabled},
    events::push_rules::PushRulesEvent,
    push::{RuleKind, Ruleset},
    RoomId,
};
use tokio::sync::RwLock;

use self::rules::{Command, Rules};

mod rules;

use crate::{error::NotificationSettingsError, event_handler::EventHandlerHandle, Client, Result};

/// Enum representing the push notification modes for a room.
#[derive(Debug, Clone, PartialEq)]
pub enum RoomNotificationMode {
    /// Receive notifications for all messages.
    AllMessages,
    /// Receive notifications for mentions and keywords only.
    MentionsAndKeywordsOnly,
    /// Do not receive any notifications.
    Mute,
}

/// A high-level API to manage the client owner's push notification settings.
#[derive(Debug, Clone)]
pub struct NotificationSettings {
    /// The underlying HTTP client.
    client: Client,
    /// Owner's account push rules. They will be updated on sync.
    rules: Arc<RwLock<Rules>>,
    /// Event handler for push rules event
    push_rules_event_handler: EventHandlerHandle,
}

impl Drop for NotificationSettings {
    fn drop(&mut self) {
        self.client.remove_event_handler(self.push_rules_event_handler.clone());
    }
}

impl NotificationSettings {
    /// Build a new `NotificationSettings``
    ///
    /// # Arguments
    ///
    /// * `client` - A `Client` used to perform API calls
    /// * `ruleset` - A `Ruleset` containing account's owner push rules
    pub fn new(client: Client, ruleset: Ruleset) -> Self {
        let rules = Arc::new(RwLock::new(Rules::new(ruleset)));

        // Listen for PushRulesEvent
        let rules_clone = rules.clone();
        let push_rules_event_handler = client.add_event_handler(move |ev: PushRulesEvent| {
            let rules = rules_clone.clone();
            async move {
                *rules.write().await = Rules::new(ev.content.global);
            }
        });

        Self { client, rules, push_rules_event_handler }
    }

    /// Gets all user defined rules matching a given `room_id`.
    async fn get_custom_rules_for_room(&self, room_id: &RoomId) -> Vec<(RuleKind, String)> {
        self.rules.read().await.get_custom_rules_for_room(room_id)
    }

    /// Gets the user defined notification mode for a room.
    pub async fn get_user_defined_room_notification_mode(
        &self,
        room_id: &RoomId,
    ) -> Option<RoomNotificationMode> {
        self.rules.read().await.get_user_defined_room_notification_mode(room_id)
    }

    /// Gets the default notification mode for a room.
    ///
    /// # Arguments
    ///
    /// * `is_encrypted` - `true` if the room is encrypted
    /// * `members_count` - the room members count
    pub async fn get_default_room_notification_mode(
        &self,
        is_encrypted: bool,
        members_count: u64,
    ) -> RoomNotificationMode {
        self.rules.read().await.get_default_room_notification_mode(is_encrypted, members_count)
    }

    /// Get whether the given ruleset contains some enabled keywords rules.
    pub async fn contains_keyword_rules(&self) -> bool {
        self.rules.read().await.contains_keyword_rules()
    }

    /// Get whether a push rule is enabled.
    pub async fn is_push_rule_enabled(
        &self,
        kind: RuleKind,
        rule_id: &str,
    ) -> Result<bool, NotificationSettingsError> {
        self.rules.read().await.is_enabled(kind, rule_id)
    }

    /// Set whether a push rule is enabled.
    pub async fn set_push_rule_enabled(
        &self,
        kind: RuleKind,
        rule_id: &str,
        enabled: bool,
    ) -> Result<(), NotificationSettingsError> {
        let rules = self.rules.read().await.clone();
        // Build the commands needed to update the push rule
        let commands = rules.build_set_enabled_commands(kind, rule_id, enabled)?;

        // Execute the commands
        self.execute_commands(&commands).await?;

        // Update the internal ruleset by applying the commands
        let rules = &mut *self.rules.write().await;
        rules.apply_commands(&commands);

        Ok(())
    }

    /// Sets the notification mode for a room.
    pub async fn set_room_notification_mode(
        &self,
        room_id: &RoomId,
        mode: RoomNotificationMode,
    ) -> Result<(), NotificationSettingsError> {
        let rules = self.rules.read().await.clone();

        // Check that the current mode is not already the target mode.
        if rules.get_user_defined_room_notification_mode(room_id) == Some(mode.clone()) {
            return Ok(());
        }

        // Build the command list to set the new mode
        let (new_rule_kind, notify) = match mode {
            RoomNotificationMode::AllMessages => {
                // insert a `Room` rule which notifies
                (RuleKind::Room, true)
            }
            RoomNotificationMode::MentionsAndKeywordsOnly => {
                // insert a `Room` rule which doesn't notify
                (RuleKind::Room, false)
            }
            RoomNotificationMode::Mute => {
                // insert an `Override` rule which doesn't notify
                (RuleKind::Override, false)
            }
        };

        let mut commands = vec![];
        if let Some(command) =
            rules.build_insert_rule_command(new_rule_kind.clone(), room_id, notify)?
        {
            commands.push(command);
        }

        // Build the command list to delete all other custom rules, with the exception
        // of the newly inserted rule
        let new_rule_id = room_id.as_str();
        let custom_rules: Vec<(RuleKind, String)> = rules
            .get_custom_rules_for_room(room_id)
            .into_iter()
            .filter(|(kind, rule_id)| kind != &new_rule_kind || rule_id != new_rule_id)
            .collect();

        if !custom_rules.is_empty() {
            commands.append(&mut rules.build_delete_rules_commands(&custom_rules)?);
        }

        // Execute the commands
        self.execute_commands(&commands).await?;

        // Update the internal ruleset by applying the commands
        let rules = &mut *self.rules.write().await;
        rules.apply_commands(&commands);

        Ok(())
    }

    /// Delete all user defined rules for a room.
    pub async fn delete_user_defined_room_rules(
        &self,
        room_id: &RoomId,
    ) -> Result<(), NotificationSettingsError> {
        let custom_rules = self.get_custom_rules_for_room(room_id).await;

        let rules = self.rules.read().await.clone();
        let commands = rules.build_delete_rules_commands(&custom_rules)?;

        // Execute the commands
        self.execute_commands(&commands).await?;

        // Update the internal ruleset by applying the commands
        let rules = &mut *self.rules.write().await;
        rules.apply_commands(&commands);

        Ok(())
    }

    /// Unmute a room.
    pub async fn unmute_room(
        &self,
        room_id: &RoomId,
        is_encrypted: bool,
        members_count: u64,
    ) -> Result<(), NotificationSettingsError> {
        // Check if there is a user defined mode
        if let Some(room_mode) = self.get_user_defined_room_notification_mode(room_id).await {
            if room_mode != RoomNotificationMode::Mute {
                // Already unmuted
                return Ok(());
            }

            // Get default mode for this room
            let default_mode =
                self.get_default_room_notification_mode(is_encrypted, members_count).await;

            // If the default mode is `Mute`, set it to `AllMessages`
            if default_mode == RoomNotificationMode::Mute {
                self.set_room_notification_mode(room_id, RoomNotificationMode::AllMessages).await
            } else {
                // Otherwise, delete user defined rules to use the default mode
                self.delete_user_defined_room_rules(room_id).await
            }
        } else {
            // This is the default mode, create a custom rule to unmute this room by setting
            // the mode to `AllMessages`
            self.set_room_notification_mode(room_id, RoomNotificationMode::AllMessages).await
        }
    }

    /// Execute a list of commands
    async fn execute_commands(
        &self,
        commands: &[Command],
    ) -> Result<(), NotificationSettingsError> {
        for command in commands {
            self.execute(command).await?;
        }
        Ok(())
    }

    /// Execute a command
    async fn execute(&self, command: &Command) -> Result<(), NotificationSettingsError> {
        match command.clone() {
            Command::DeletePushRule { scope, kind, rule_id } => {
                let request = delete_pushrule::v3::Request::new(scope, kind, rule_id);
                self.client
                    .send(request, None)
                    .await
                    .map_err(|_| NotificationSettingsError::UnableToRemovePushRule)?;
            }
            Command::SetPushRule { scope, rule } => {
                let request = set_pushrule::v3::Request::new(scope, rule);
                self.client
                    .send(request, None)
                    .await
                    .map_err(|_| NotificationSettingsError::UnableToAddPushRule)?;
            }
            Command::SetPushRuleEnabled { scope, kind, rule_id, enabled } => {
                let request = set_pushrule_enabled::v3::Request::new(scope, kind, rule_id, enabled);
                self.client
                    .send(request, None)
                    .await
                    .map_err(|_| NotificationSettingsError::UnableToUpdatePushRule)?;
            }
        }
        Ok(())
    }
}

// The http mocking library is not supported for wasm32
#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod tests {
    use assert_matches::assert_matches;
    use matrix_sdk_test::async_test;
    use ruma::{
        push::{
            Action, AnyPushRuleRef, NewPatternedPushRule, NewPushRule, PredefinedOverrideRuleId,
            PredefinedUnderrideRuleId, RuleKind,
        },
        OwnedRoomId, RoomId,
    };
    use wiremock::{http::Method, matchers::method, Mock, MockServer, ResponseTemplate};

    use super::rules::{Command, Rules};
    use crate::{
        error::NotificationSettingsError,
        notification_settings::{NotificationSettings, RoomNotificationMode},
        test_utils::logged_in_client,
    };

    fn get_test_room_id() -> OwnedRoomId {
        RoomId::parse("!AAAaAAAAAaaAAaaaaa:matrix.org").unwrap()
    }

    fn insert_room_rule(
        rules: &mut Rules,
        kind: RuleKind,
        room_id: &RoomId,
        notify: bool,
    ) -> Result<Option<Command>, NotificationSettingsError> {
        let command = rules.build_insert_rule_command(kind, room_id, notify)?;
        if let Some(inner) = &command {
            rules.apply(inner);
        }
        Ok(command)
    }

    async fn update_rules(notification_settings: &NotificationSettings, rules: &Rules) {
        *notification_settings.rules.write().await = rules.clone();
    }

    #[async_test]
    async fn test_get_custom_rules_for_room() {
        let server = MockServer::start().await;
        let client = logged_in_client(Some(server.uri())).await;
        let room_id = get_test_room_id();

        let notification_settings = client.notification_settings().await;

        assert!(notification_settings.get_custom_rules_for_room(&room_id).await.is_empty());

        let mut rules = notification_settings.rules.read().await.clone();
        _ = insert_room_rule(&mut rules, RuleKind::Room, &room_id, true).unwrap();
        update_rules(&notification_settings, &rules).await;
        assert_eq!(notification_settings.get_custom_rules_for_room(&room_id).await.len(), 1);

        _ = insert_room_rule(&mut rules, RuleKind::Override, &room_id, true).unwrap();
        update_rules(&notification_settings, &rules).await;
        assert_eq!(notification_settings.get_custom_rules_for_room(&room_id).await.len(), 2);
    }

    #[async_test]
    async fn test_get_user_defined_room_notification_mode() {
        let server = MockServer::start().await;
        let client = logged_in_client(Some(server.uri())).await;
        let room_id = get_test_room_id();

        let notification_settings = client.notification_settings().await;
        assert!(notification_settings
            .get_user_defined_room_notification_mode(&room_id)
            .await
            .is_none());

        let mut rules = notification_settings.rules.read().await.clone();
        // Set a notifying `Room` rule into the ruleset to be in `AllMessages`
        _ = insert_room_rule(&mut rules, RuleKind::Room, &room_id, true).unwrap();
        update_rules(&notification_settings, &rules).await;
        assert_eq!(
            notification_settings.get_user_defined_room_notification_mode(&room_id).await.unwrap(),
            RoomNotificationMode::AllMessages
        );

        // Set a mute `Room` rule into the ruleset to be in `MentionsAndKeywordsOnly`
        _ = insert_room_rule(&mut rules, RuleKind::Room, &room_id, false).unwrap();
        update_rules(&notification_settings, &rules).await;
        assert_eq!(
            notification_settings.get_user_defined_room_notification_mode(&room_id).await.unwrap(),
            RoomNotificationMode::MentionsAndKeywordsOnly
        );

        // Set a mute `Override` rule into the ruleset to be in `Mute`
        _ = insert_room_rule(&mut rules, RuleKind::Override, &room_id, false).unwrap();
        update_rules(&notification_settings, &rules).await;
        assert_eq!(
            notification_settings.get_user_defined_room_notification_mode(&room_id).await.unwrap(),
            RoomNotificationMode::Mute
        );
    }

    #[async_test]
    async fn test_get_default_room_notification_mode() {
        let server = MockServer::start().await;
        let client = logged_in_client(Some(server.uri())).await;

        let notification_settings = client.notification_settings().await;
        let mut rules = notification_settings.rules.read().await.clone();

        rules
            .ruleset
            .set_actions(
                RuleKind::Underride,
                PredefinedUnderrideRuleId::RoomOneToOne,
                vec![Action::Notify],
            )
            .unwrap();
        update_rules(&notification_settings, &rules).await;
        assert_eq!(
            notification_settings.get_default_room_notification_mode(false, 2).await,
            RoomNotificationMode::AllMessages
        );

        rules
            .ruleset
            .set_actions(RuleKind::Underride, PredefinedUnderrideRuleId::RoomOneToOne, vec![])
            .unwrap();
        update_rules(&notification_settings, &rules).await;
        assert_eq!(
            notification_settings.get_default_room_notification_mode(false, 2).await,
            RoomNotificationMode::MentionsAndKeywordsOnly
        );

        rules
            .ruleset
            .set_enabled(RuleKind::Underride, PredefinedUnderrideRuleId::RoomOneToOne, false)
            .unwrap();
        update_rules(&notification_settings, &rules).await;
        assert_eq!(
            notification_settings.get_default_room_notification_mode(false, 2).await,
            RoomNotificationMode::MentionsAndKeywordsOnly
        );
    }

    #[async_test]
    async fn test_contains_keyword_rules() {
        let server = MockServer::start().await;
        let client = logged_in_client(Some(server.uri())).await;

        let notification_settings = client.notification_settings().await;
        let mut rules = notification_settings.rules.read().await.clone();

        let contains_keywords_rules = notification_settings.contains_keyword_rules().await;
        assert!(!contains_keywords_rules);

        let rule = NewPatternedPushRule::new(
            "keyword_rule_id".into(),
            "keyword".into(),
            vec![Action::Notify],
        );

        rules.ruleset.insert(NewPushRule::Content(rule), None, None).unwrap();
        update_rules(&notification_settings, &rules).await;

        let contains_keywords_rules = notification_settings.contains_keyword_rules().await;
        assert!(contains_keywords_rules);
    }

    #[async_test]
    async fn test_is_push_rule_enabled() {
        let server = MockServer::start().await;
        let client = logged_in_client(Some(server.uri())).await;

        // Initial state: Reaction disabled
        let mut ruleset = client.account().push_rules().await.unwrap();
        ruleset.set_enabled(RuleKind::Override, PredefinedOverrideRuleId::Reaction, false).unwrap();

        let notification_settings = NotificationSettings::new(client.clone(), ruleset);

        let enabled = notification_settings
            .is_push_rule_enabled(RuleKind::Override, PredefinedOverrideRuleId::Reaction.as_str())
            .await
            .unwrap();

        assert!(!enabled);

        // Initial state: Reaction enabled
        let mut ruleset = client.account().push_rules().await.unwrap();
        ruleset.set_enabled(RuleKind::Override, PredefinedOverrideRuleId::Reaction, true).unwrap();

        let notification_settings = NotificationSettings::new(client, ruleset);

        let enabled = notification_settings
            .is_push_rule_enabled(RuleKind::Override, PredefinedOverrideRuleId::Reaction.as_str())
            .await
            .unwrap();

        assert!(enabled);
    }

    #[async_test]
    async fn test_set_push_rule_enabled() {
        let server = MockServer::start().await;
        let client = logged_in_client(Some(server.uri())).await;
        let mut ruleset = client.account().push_rules().await.unwrap();
        // Initial state
        ruleset
            .set_enabled(RuleKind::Override, PredefinedOverrideRuleId::IsUserMention, false)
            .unwrap();

        let notification_settings = NotificationSettings::new(client, ruleset);

        Mock::given(method("PUT")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

        notification_settings
            .set_push_rule_enabled(
                RuleKind::Override,
                PredefinedOverrideRuleId::IsUserMention.as_str(),
                true,
            )
            .await
            .unwrap();

        // The ruleset must have been updated
        let rules = notification_settings.rules.read().await;
        let rule =
            rules.ruleset.get(RuleKind::Override, PredefinedOverrideRuleId::IsUserMention).unwrap();
        assert!(rule.enabled());
    }

    #[async_test]
    async fn test_set_push_rule_enabled_api_error() {
        let server = MockServer::start().await;
        let client = logged_in_client(Some(server.uri())).await;
        let mut ruleset = client.account().push_rules().await.unwrap();
        // Initial state
        ruleset
            .set_enabled(RuleKind::Override, PredefinedOverrideRuleId::IsUserMention, false)
            .unwrap();

        let notification_settings = NotificationSettings::new(client, ruleset);

        // If the server returns an error
        Mock::given(method("PUT")).respond_with(ResponseTemplate::new(500)).mount(&server).await;

        // When enabling the push rule
        assert_eq!(
            notification_settings
                .set_push_rule_enabled(
                    RuleKind::Override,
                    PredefinedOverrideRuleId::IsUserMention.as_str(),
                    true,
                )
                .await,
            Err(NotificationSettingsError::UnableToUpdatePushRule)
        );

        // The ruleset must not have been updated
        let rules = notification_settings.rules.read().await;
        let rule =
            rules.ruleset.get(RuleKind::Override, PredefinedOverrideRuleId::IsUserMention).unwrap();
        assert!(!rule.enabled());
    }

    #[async_test]
    async fn test_set_room_notification_mode() {
        let server = MockServer::start().await;
        let client = logged_in_client(Some(server.uri())).await;

        Mock::given(method("PUT")).respond_with(ResponseTemplate::new(200)).mount(&server).await;
        Mock::given(method("DELETE")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

        let notification_settings = client.notification_settings().await;
        let room_id = get_test_room_id();

        let mode = notification_settings.get_user_defined_room_notification_mode(&room_id).await;
        assert!(mode.is_none());

        let new_modes = &[
            RoomNotificationMode::AllMessages,
            RoomNotificationMode::MentionsAndKeywordsOnly,
            RoomNotificationMode::Mute,
        ];
        for new_mode in new_modes {
            notification_settings
                .set_room_notification_mode(&room_id, new_mode.clone())
                .await
                .unwrap();

            assert_eq!(
                new_mode.clone(),
                notification_settings
                    .get_user_defined_room_notification_mode(&room_id)
                    .await
                    .unwrap()
            );
        }
    }

    #[async_test]
    async fn test_set_room_notification_mode_requests_order() {
        let server = MockServer::start().await;
        let client = logged_in_client(Some(server.uri())).await;

        Mock::given(method("PUT")).respond_with(ResponseTemplate::new(200)).mount(&server).await;
        Mock::given(method("DELETE")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

        let notification_settings = client.notification_settings().await;
        let room_id = get_test_room_id();

        // Set the initial state to `AllMessages` by setting a `Room` rule that notifies
        let mut rules = notification_settings.rules.read().await.clone();
        _ = insert_room_rule(&mut rules, RuleKind::Room, &room_id, true);
        update_rules(&notification_settings, &rules).await;

        // Set the new mode to `Mute`, this will add a new `Override` rule without
        // action and remove the `Room` rule.
        notification_settings
            .set_room_notification_mode(&room_id, RoomNotificationMode::Mute)
            .await
            .unwrap();

        assert_eq!(
            RoomNotificationMode::Mute,
            notification_settings.get_user_defined_room_notification_mode(&room_id).await.unwrap()
        );

        // Test that the PUT is executed before the DELETE, so that the following sync
        // results will give the following transitions: `AllMessages` ->
        // `AllMessages` -> `Mute` by sending the DELETE before the PUT, we
        // would have `AllMessages` -> `Default` -> `Mute`
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, Method::Put);
        assert_eq!(requests[1].method, Method::Delete);
    }

    #[async_test]
    async fn test_set_room_notification_mode_api_error() {
        let server = MockServer::start().await;
        let client = logged_in_client(Some(server.uri())).await;

        // If the server returns an error
        Mock::given(method("PUT")).respond_with(ResponseTemplate::new(500)).mount(&server).await;
        Mock::given(method("DELETE")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

        let notification_settings = client.notification_settings().await;
        let room_id = get_test_room_id();

        let mode = notification_settings.get_user_defined_room_notification_mode(&room_id).await;
        assert!(mode.is_none());

        // Setting the new mode should fail
        assert_eq!(
            Err(NotificationSettingsError::UnableToAddPushRule),
            notification_settings
                .set_room_notification_mode(&room_id, RoomNotificationMode::AllMessages)
                .await
        );

        // The ruleset must not have been updated
        assert!(notification_settings
            .get_user_defined_room_notification_mode(&room_id)
            .await
            .is_none());
    }

    #[async_test]
    async fn test_delete_user_defined_room_rules() {
        let server = MockServer::start().await;
        let client = logged_in_client(Some(server.uri())).await;
        let room_id_a = RoomId::parse("!AAAaAAAAAaaAAaaaaa:matrix.org").unwrap();
        let room_id_b = RoomId::parse("!BBBbBBBBBbbBBbbbbb:matrix.org").unwrap();

        Mock::given(method("DELETE")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

        let notification_settings = client.notification_settings().await;

        // Insert some initial rules
        let mut rules = notification_settings.rules.read().await.clone();
        _ = insert_room_rule(&mut rules, RuleKind::Room, &room_id_a, true).unwrap();
        _ = insert_room_rule(&mut rules, RuleKind::Override, &room_id_a, true).unwrap();
        _ = insert_room_rule(&mut rules, RuleKind::Room, &room_id_b, true).unwrap();
        _ = insert_room_rule(&mut rules, RuleKind::Override, &room_id_b, true).unwrap();
        update_rules(&notification_settings, &rules).await;

        // Delete all user defined rules for room_id_a
        notification_settings.delete_user_defined_room_rules(&room_id_a).await.unwrap();

        // Only the rules for room_id_b should remain
        let remaining_rules = notification_settings.get_custom_rules_for_room(&room_id_b).await;
        assert_eq!(remaining_rules.len(), 2);
        assert!(notification_settings.get_custom_rules_for_room(&room_id_a).await.is_empty());
    }

    #[async_test]
    async fn test_unmute_room_not_muted() {
        let server = MockServer::start().await;
        let client = logged_in_client(Some(server.uri())).await;
        let room_id = get_test_room_id();
        let notification_settings = client.notification_settings().await;
        let mut rules = notification_settings.rules.read().await.clone();

        // Initialize with a `MentionsAndKeywordsOnly` mode
        _ = insert_room_rule(&mut rules, RuleKind::Room, &room_id, false).unwrap();
        update_rules(&notification_settings, &rules).await;

        notification_settings.unmute_room(&room_id, true, 2).await.unwrap();

        // The ruleset must not be modified
        let room_rules = notification_settings.get_custom_rules_for_room(&room_id).await;
        assert_eq!(room_rules.len(), 1);
        assert_matches!(rules.ruleset.get(RuleKind::Room, &room_id),
            Some(AnyPushRuleRef::Room(rule)) => {
                assert_eq!(rule.rule_id, room_id);
                assert!(rule.actions.is_empty());
            }
        );
    }

    #[async_test]
    async fn test_unmute_room() {
        let server = MockServer::start().await;
        Mock::given(method("PUT")).respond_with(ResponseTemplate::new(200)).mount(&server).await;
        Mock::given(method("DELETE")).respond_with(ResponseTemplate::new(200)).mount(&server).await;
        let client = logged_in_client(Some(server.uri())).await;
        let room_id = get_test_room_id();
        let notification_settings = client.notification_settings().await;

        // Start with the room muted
        notification_settings
            .set_room_notification_mode(&room_id, RoomNotificationMode::Mute)
            .await
            .unwrap();
        assert_eq!(
            Some(RoomNotificationMode::Mute),
            notification_settings.get_user_defined_room_notification_mode(&room_id).await
        );

        // Unmute the room
        notification_settings.unmute_room(&room_id, false, 2).await.unwrap();

        // The user defined mode must have been removed
        assert!(notification_settings
            .get_user_defined_room_notification_mode(&room_id)
            .await
            .is_none());
    }

    #[async_test]
    async fn test_unmute_room_default_mode() {
        let server = MockServer::start().await;
        Mock::given(method("PUT")).respond_with(ResponseTemplate::new(200)).mount(&server).await;
        let client = logged_in_client(Some(server.uri())).await;
        let room_id = get_test_room_id();
        let notification_settings = client.notification_settings().await;

        // Unmute the room
        notification_settings.unmute_room(&room_id, false, 2).await.unwrap();

        // The new mode must be `AllMessages`
        assert_eq!(
            Some(RoomNotificationMode::AllMessages),
            notification_settings.get_user_defined_room_notification_mode(&room_id).await
        );
    }
}
