-- Safe to run against an existing source database.
-- Historical/backfill updates may provide an explicit event-time in updated_at.
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at THEN
        NEW.updated_at = timezone('UTC', now());
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
