\timing on

DROP TABLE IF EXISTS exclude_from_batch;
CREATE TABLE exclude_from_batch (record BIGINT, reason TEXT);

INSERT INTO exclude_from_batch (record,reason) 
SELECT record, 'has parts' FROM biblio.monograph_part WHERE NOT deleted
AND EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'skip parts' AND value = 'TRUE' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1));

INSERT INTO exclude_from_batch (record,reason) 
SELECT record, 'acq record' FROM asset.call_number WHERE NOT deleted  AND id IN (SELECT call_number FROM asset.copy WHERE barcode ~* 'acq' AND NOT deleted)
AND EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'skip acq records' AND value = 'TRUE' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1));

INSERT INTO exclude_from_batch (record,reason) 
SELECT DISTINCT acn.record, 'circ mod exclude' FROM asset.call_number acn JOIN asset.copy acp ON acp.call_number = acn.id WHERE NOT acp.deleted 
AND acp.circ_modifier IN (SELECT BTRIM(value) FROM dedupe_features WHERE name = 'exclude_circ_mod' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1));

INSERT INTO exclude_from_batch (record,reason)
SELECT DISTINCT acn.record, 'shelving location exclude' FROM asset.call_number acn JOIN asset.copy acp ON acp.call_number = acn.id 
JOIN asset.copy_location acl ON acl.id = acp.location 
WHERE NOT acp.deleted
AND BTRIM(acl.name) IN (SELECT BTRIM(value) FROM dedupe_features WHERE name = 'exclude_copy_location' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1));

DO $$
DECLARE 
    feature_id INTEGER;
    tag_value TEXT;
    str TEXT;
    report_count INTEGER DEFAULT 0;
BEGIN
    FOR feature_id IN SELECT id FROM dedupe_features WHERE name = 'exclude_tag_value' LOOP
		SELECT value, value2 FROM dedupe_features WHERE id = feature_id INTO tag_value, str;
        IF LENGTH(tag_value) != 3 THEN
            RAISE NOTICE 'not processing tag % because it is not three characters long', tag_value;
            EXIT;
        END IF;
		INSERT INTO exclude_from_batch (record, reason) 
			SELECT DISTINCT record, 'tag exclusion' FROM metabib.real_full_rec WHERE tag ~* tag_value 
			AND value ~* str AND record IN (SELECT id FROM biblio.record_entry WHERE NOT deleted);
	END LOOP;
    SELECT COUNT(*) FROM exclude_from_batch WHERE reason = 'tag exclusion' INTO report_count;
    RAISE INFO 'excluded due to tag values %', report_count;
END $$;

INSERT INTO exclude_from_batch (record,reason)
SELECT value::BIGINT, 'banned list' FROM dedupe_features WHERE name = 'banned bib' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1);

INSERT INTO exclude_from_batch (record,reason) 
SELECT id, 'safe harbor' FROM biblio.record_entry WHERE NOT deleted AND create_date > NOW() - (SELECT value FROM dedupe_features WHERE name = 'exclude_safe_haror' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1))::INTERVAL;

INSERT INTO exclude_from_batch (record,reason)
SELECT record, 'excluded search format' FROM dedupe_batch WHERE search_format && (SELECT array_agg(value) FROM dedupe_features WHERE name = 'exclude_search_format' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1));

INSERT INTO exclude_from_batch (record,reason)
SELECT record, 'no search format' FROM dedupe_batch WHERE search_format IS NULL 
AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'exclude_null_search_format' AND value = 'TRUE' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1));

DO $$ 
DECLARE
    feature_id INTEGER;
    partial_label TEXT;
    str TEXT;
    report_count INTEGER DEFAULT 0;
BEGIN
    FOR feature_id IN SELECT id FROM dedupe_features WHERE name = 'exclude_partial_call_number' LOOP
        SELECT value, value2 FROM dedupe_features WHERE id = feature_id INTO partial_label, str;
        INSERT INTO exclude_from_batch (record,reason)
            SELECT DISTINCT acn.record, 'partial call number' FROM asset.call_number acn 
            WHERE NOT acn.deleted AND acn.label LIKE ('%' || partial_label || '%')
            AND acn.record IN (SELECT record FROM dedupe_batch);
    END LOOP;
    SELECT COUNT(*) FROM exclude_from_batch WHERE reason = 'partial call number' INTO report_count;
    RAISE INFO 'excluded due to partial call number %', report_count;
END $$;

DO $$
DECLARE
    feature_id INTEGER;
    sf TEXT;
    str TEXT;
    report_count INTEGER DEFAULT 0;
BEGIN
    FOR feature_id IN SELECT id FROM dedupe_features WHERE name = 'restrict_sf_match_by_cm' LOOP
        SELECT value, value2 FROM dedupe_features WHERE id = feature_id INTO sf, str;
		INSERT INTO exclude_from_batch (record,reason)
			SELECT DISTINCT acn.record, 'restricted by circ mod' FROM asset.call_number acn JOIN asset.copy acp ON acp.call_number = acn.id WHERE NOT acp.deleted 
			AND acp.circ_modifier NOT IN (SELECT value2 FROM dedupe_features WHERE name = 'restrict_sf_match_by_cm' AND value = sf AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1))
			AND acn.record IN (SELECT record FROM dedupe_batch WHERE sf = ANY(search_format));
    END LOOP;
    SELECT COUNT(*) FROM exclude_from_batch WHERE reason = 'restricted by circ mod' INTO report_count;
    RAISE INFO 'excluded due to circ mod %', report_count;
END $$;

DROP TABLE IF EXISTS copy_level_strings;

DO $$ 
DECLARE
	sfs TEXT[];
BEGIN
	SELECT ARRAY_AGG(DISTINCT value) FROM dedupe_features WHERE name = 'restrict_sf_match_by_string' 
	AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1) INTO sfs;

	CREATE UNLOGGED TABLE copy_level_strings AS 
	SELECT acn.record, acp.id AS acp_id, CONCAT_WS(' ',acnp.label,acn.label,acns.label) AS label, acl.name AS location 
	FROM asset.call_number acn 
	JOIN asset.copy acp ON acp.call_number = acn.id 
	JOIN asset.copy_location acl ON acl.id = acp.location
	JOIN asset.call_number_suffix acns ON acns.id = acn.suffix
	JOIN asset.call_number_prefix acnp ON acnp.id = acn.prefix
	WHERE NOT acp.deleted AND acn.record IN (SELECT record FROM dedupe_batch WHERE search_format && sfs);
END $$;

SELECT COUNT(*) FROM copy_level_strings;
ALTER TABLE copy_level_strings ADD COLUMN keep BOOLEAN DEFAULT FALSE;

DO $$ 
DECLARE
    feature_id INTEGER;
    sf TEXT;
    str TEXT;
    row_count INTEGER DEFAULT 0;
BEGIN
    FOR feature_id IN SELECT id FROM dedupe_features WHERE name = 'restrict_sf_match_by_string' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1) LOOP
        SELECT value, value2 FROM dedupe_features WHERE id = feature_id INTO sf, str;
        SELECT COUNT(*) FROM copy_level_strings
            WHERE record IN (SELECT record FROM dedupe_batch WHERE sf = ANY(search_format))
            AND (label ~* str OR location ~* str);
	    UPDATE copy_level_strings SET keep = TRUE 
		    WHERE record IN (SELECT record FROM dedupe_batch WHERE sf = ANY(search_format))
		    AND (label ~* str OR location ~* str);
    END LOOP;
    RAISE NOTICE 'values in copy_level_strings set to keep : %', row_count;
END $$;

INSERT INTO exclude_from_batch (record,reason) SELECT DISTINCT record, 'restricted by string' 
FROM copy_level_strings WHERE NOT keep;

DELETE FROM dedupe_batch WHERE record IN (SELECT DISTINCT record FROM exclude_from_batch);

DROP TABLE copy_level_strings;
