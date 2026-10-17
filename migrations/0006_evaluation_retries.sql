-- Keep transient expansion failures retryable without monopolizing the due batch.
-- Existing schedules remain immediately eligible at their original cursor.
ALTER TABLE schedules ADD COLUMN evaluation_retry_at TIMESTAMPTZ;
CREATE INDEX schedules_evaluation_due
    ON schedules (COALESCE(evaluation_retry_at, next_fire_at), id)
    WHERE status = 'active';
