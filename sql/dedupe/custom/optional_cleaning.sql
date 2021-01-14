\timing on

--did any records get deleted in the interim?
DROP TABLE IF EXISTS interim_deleted_bibs;
CREATE TABLE interim_deleted_bibs AS 
SELECT id, 'lead'::TEXT AS record_type, 1 AS pass FROM biblio.record_entry WHERE deleted = TRUE AND id IN (SELECT lead_record FROM groups);

INSERT INTO interim_deleted_bibs (id,record_type,pass)  SELECT id, 'follower', 1 FROM biblio.record_entry WHERE deleted = TRUE AND id IN (SELECT UNNEST(records) FROM groups);

SELECT COUNT(bibs.id), bibs.record_type, bre.creator FROM interim_deleted_bibs bibs JOIN biblio.record_entry bre ON bre.id = bibs.id GROUP BY 2, 3;

UPDATE groups SET lead_record = NULL WHERE lead_record IN (SELECT id FROM interim_deleted_bibs);
UPDATE groups SET records = ANYARRAY_REMOVE(records,(SELECT ARRAY_AGG(id) FROM interim_deleted_bibs WHERE record_type = 'follower'));

DO $$
DECLARE
    x   INTEGER;
    r   INTEGER;
    s   INTEGER;
BEGIN
    FOR x IN SELECT id FROM groups WHERE lead_record IS NULL LOOP
        s = NULL;
        r = NULL;
        SELECT b.record, b.score
            FROM (SELECT id, UNNEST(records) AS record FROM groups WHERE id = x) q
            JOIN dedupe_batch b ON b.record = q.record
            ORDER BY b.score DESC, b.record DESC
            LIMIT 1 INTO r, s;
        UPDATE groups SET score = s, lead_record = r WHERE id = x;
        UPDATE groups SET records = ANYARRAY_REMOVE(records,lead_record) WHERE id = x;
    END LOOP;
END $$;
SELECT COUNT(*) FROM groups WHERE lead_record IS NULL;

DELETE FROM groups WHERE records = '{}';

DROP TABLE IF EXISTS ancient_holds;
CREATE TABLE ancient_holds (id INTEGER, hold_type TEXT, target BIGINT);
INSERT INTO ancient_holds (id, hold_type, target) SELECT id, hold_type, target FROM action.hold_request 
	WHERE capture_time IS NULL AND fulfillment_time IS NULL AND cancel_time IS NULL AND hold_type = 'T' AND target IN (SELECT lead_record FROM groups)
	AND request_time < NOW() - (SELECT value FROM dedupe_features WHERE org = (SELECT shortname FROM actor.org_unit WHERE id = 1) AND name = 'remove aged holds')::INTERVAL;

UPDATE action.hold_request SET cancel_time = NOW(), cancel_note = 'very old hold canceled during dedupe', cancel_cause = 5 WHERE id IN (SELECT id FROM ancient_holds);

-- let's clean up abandoned holdings, confuses people in testing 

SELECT * FROM create_bre_no_holdings();
SELECT * FROM create_acn_no_holdings();

UPDATE asset.call_number SET deleted = TRUE WHERE id IN (SELECT id FROM acn_no_holdings) 
	AND EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'remove childless volumes' AND value IS NOT NULL AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1));
UPDATE biblio.record_entry SET deleted = TRUE WHERE id IN (SELECT id FROM bre_no_holdings)
	AND EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'remove childless bibs' AND value IS NOT NULL AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1));

