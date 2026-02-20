import re

with open("src/bot_vstrechi/db/repository.py", "r") as f:
    content = f.read()

old_query = """                SELECT
                    id,
                    calendar_id,
                    external_event_id,
                    resource_state,
                    message_number,
                    run_after,
                    attempts
                FROM calendar_sync_signal
                WHERE status = ? AND run_after <= ?
                ORDER BY run_after ASC, id ASC
                LIMIT 1
                ",
                (OutboxStatus.PENDING, _serialize_datetime(now)),"""

new_query = """                SELECT
                    id,
                    calendar_id,
                    external_event_id,
                    resource_state,
                    message_number,
                    run_after,
                    attempts
                FROM calendar_sync_signal
                WHERE status = ? AND run_after <= ?
                  AND calendar_id NOT IN (
                      SELECT calendar_id FROM calendar_sync_signal WHERE status = ?
                  )
                ORDER BY run_after ASC, id ASC
                LIMIT 1
                ",
                (OutboxStatus.PENDING, _serialize_datetime(now), OutboxStatus.RUNNING),"""

content = content.replace(old_query, new_query)
with open("src/bot_vstrechi/db/repository.py", "w") as f:
    f.write(content)

