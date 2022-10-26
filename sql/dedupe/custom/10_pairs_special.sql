\timing on

DO $$
BEGIN
    EXECUTE format('SET %I TO %L', 'var.description_page_range', (dedupe_setting('page variation')::INTEGER));
END $$;

DO $$
BEGIN
    EXECUTE format('SET %I TO %L', 'var.description_cm_range', (dedupe_setting('cm variation')::INTEGER));
END $$;

SELECT 'page range is ', current_setting('var.description_page_range')::INTEGER;
SELECT 'cm range is ', current_setting('var.description_cm_range')::INTEGER;

/*
-- FOR SCLENDS 
DROP TABLE IF EXISTS local_history_bibs;
CREATE TABLE local_history_bibs AS 
SELECT DISTINCT record FROM asset.call_number WHERE deleted = FALSE AND id IN (
    SELECT DISTINCT call_number FROM asset.copy WHERE deleted = FALSE AND location IN (SELECT * FROM find_locations_by_names('derived in - local','derived not in - local') ));
CREATE INDEX local_history_bibs_recordx ON local_history_bibs(record);

ALTER TABLE local_history_bibs ADD COLUMN search_format TEXT[], ADD COLUMN title TEXT;
UPDATE local_history_bibs a SET search_format = b.search_format, title = b.title FROM dedupe_batch b WHERE a.record = b.record;
CREATE INDEX local_history_bibs_titlex ON local_history_bibs(title);

INSERT INTO pairs (merge_set,records,match_set)
SELECT
    'local history'
    ,ARRAY[a.record,b.record]
    ,a.match_set
FROM
    dedupe_batch a
JOIN
    dedupe_batch b ON b.title = a.title
WHERE
    a.record != b.record
    AND a.author = b.author
    AND a.can_have_copies
    AND b.can_have_copies
    AND a.search_format_str = b.search_format_str
    AND (a.record > b.record OR (SELECT COALESCE(value::BIGINT,1) FROM dedupe_features WHERE name = 'incoming floor' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1)) != 1)
    AND 'book' = ANY(a.search_format) 
    AND 'book' = ANY(b.search_format)
    AND a.title IN (SELECT title FROM sc_local_history_bibs)
    AND (
          (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
          (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
    AND EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'merge local history' AND value = 'TRUE' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1))
;
*/

-- ocls match set
INSERT INTO pairs (merge_set,records,match_set)
SELECT
    'tacs oclc'
    ,ARRAY[a.record,b.record]
    ,a.title
FROM
    dedupe_batch a
JOIN
    dedupe_batch b ON b.title = a.title AND a.search_format_str = b.search_format_str
WHERE
    a.record != b.record
    AND a.author = b.author
    AND a.can_have_copies
    AND b.can_have_copies
    AND ( 
          (a.avdisc_flag = FALSE AND b.avdisc_flag = FALSE)
            OR
          (a.avdisc_flag AND b.avdisc_flag AND (a.description = b.description OR (a.description IS NULL AND b.description IS NULL)))
        )
    AND a.oclc_values && b.oclc_values
    AND a.manga = FALSE AND b.manga = FALSE
    AND ((a.titlepart = b.titlepart) OR (a.titlepart IS NULL AND b.titlepart IS NULL))
    AND ((a.titlepartname = b.titlepartname) OR (a.titlepartname IS NULL AND b.titlepartname IS NULL))
    AND ( 
          (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
          (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
    AND dedupe_setting('merge tacs oclc') = 'TRUE'
;

