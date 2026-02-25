# APPLICATION LAYER - Bot Vstrechi

## OVERVIEW
Orchestrates meeting workflows by coordinating domain logic commands with database and external API adapters.

## STRUCTURE
- `service.py`: Contains `MeetingWorkflowService`, the core orchestrator for the entire application.
- `MeetDraftSession`: Tracks temporary meeting data during the creation process.
- `DaySlotOption`: Represents available time slots retrieved from the calendar gateway.

## WHERE TO LOOK
| Logic Area | Method / Location | Purpose |
|------------|-------------------|---------|
| State Transitions | `_apply_with_guard` | Executes commands and ensures atomic persistence. |
| User Discovery | `_resolve_participant_mappings` | Fuzzy matching for Telegram usernames and emails. |
| Slot Selection | `find_free_slots` | Searches Google Calendar for available meeting times. |
| Outbox Enqueue | `_enqueue_transition_notifications` | Schedules Telegram messages after state changes. |
| Calendar Sync | `_enqueue_calendar_sync` | Pushes local state changes back to Google Calendar. |
| Participant Sync | `sync_participants_from_calendar` | Add/remove required participants and suppress stale outbox/tokens. |

## CONVENTIONS
- **Manual Dependency Injection:** Dependencies like the Repository and CalendarGateway are passed directly to the constructor.
- **Atomic Persistence:** Every operation that modifies state or enqueues Outbox effects must run inside a `self._repository.atomic()` block.
- **Error Handling:** Catch exceptions at the service level. Log the failure, then return an appropriate `CommandExecution` or `CommandResult` response.
- **Deterministic Time:** All time-sensitive methods must accept a `now` parameter to ensure consistency and testability.
- **Two-phase terminal status:** For bot-managed meetings with Google event id, publish final group status after successful calendar patch (via outbox chain).
- **Delegated audit fields:** When manager acts via initiator callbacks, keep `requested_by_user_id` and `effective_actor_user_id` in audit details.

## ANTI-PATTERNS
- **Direct API Calls:** Never call Telegram or Google APIs directly. Use provided gateways or the Outbox pattern.
- **Direct Business Rules:** Don't put business logic inside the service. Use domain commands to compute state changes.
- **Partial Updates:** Avoid repository calls that aren't wrapped in an atomic transaction if they belong to one operation.
