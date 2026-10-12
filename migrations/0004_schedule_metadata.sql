-- No rewrite/backfill: legacy specs deserialize missing metadata as an empty map.
-- This migration builds an index while holding a write lock; plan a maintenance
-- window for large installations, as with the other embedded migrations.
CREATE INDEX schedules_metadata ON schedules USING GIN ((spec -> 'metadata') jsonb_path_ops);
