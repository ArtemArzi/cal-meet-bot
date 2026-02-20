import re

with open("src/bot_vstrechi/application/service.py", "r") as f:
    content = f.read()

old_apply = """        applied = self._repository.apply_execution(
            before=before, execution=execution, now=now
        )
        if applied:
            self._enqueue_transition_notifications(
                before=before,
                after=execution.meeting,
                now=now,
            )
            if action == "record_participant_decision" and actor_user_id is not None:
                self._enqueue_pending_progress_update(
                    before=before,
                    after=execution.meeting,
                    actor_user_id=actor_user_id,
                    now=now,
                )
            self._enqueue_calendar_sync(
                before=before,
                after=execution.meeting,
                action=action,
                actor_user_id=actor_user_id,
                now=now,
            )
            self._write_audit_log(
                before=before,
                execution=execution,
                action=action,
                actor_user_id=actor_user_id,
                now=now,
            )
            logger.info(
                "command executed",
                extra={
                    "meeting_id": execution.meeting.meeting_id,
                    "outcome": execution.result.outcome,
                    "reason_code": execution.result.reason_code,
                    "state_before": before.state,
                    "state_after": execution.meeting.state,
                },
            )
            return execution"""

new_apply = """        with self._repository.atomic():
            applied = self._repository.apply_execution(
                before=before, execution=execution, now=now
            )
            if applied:
                self._enqueue_transition_notifications(
                    before=before,
                    after=execution.meeting,
                    now=now,
                )
                if action == "record_participant_decision" and actor_user_id is not None:
                    self._enqueue_pending_progress_update(
                        before=before,
                        after=execution.meeting,
                        actor_user_id=actor_user_id,
                        now=now,
                    )
                self._enqueue_calendar_sync(
                    before=before,
                    after=execution.meeting,
                    action=action,
                    actor_user_id=actor_user_id,
                    now=now,
                )
                self._write_audit_log(
                    before=before,
                    execution=execution,
                    action=action,
                    actor_user_id=actor_user_id,
                    now=now,
                )
                logger.info(
                    "command executed",
                    extra={
                        "meeting_id": execution.meeting.meeting_id,
                        "outcome": execution.result.outcome,
                        "reason_code": execution.result.reason_code,
                        "state_before": before.state,
                        "state_after": execution.meeting.state,
                    },
                )
                return execution"""

content = content.replace(old_apply, new_apply)

with open("src/bot_vstrechi/application/service.py", "w") as f:
    f.write(content)
