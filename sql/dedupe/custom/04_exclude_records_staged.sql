\timing on

-- migration stuff is in DO loops to prevent errors 

ALTER TABLE exclude_from_batch ALTER COLUMN staged SET DEFAULT TRUE;

DO $$
DECLARE
    use_staged BOOLEAN DEFAULT FALSE;
    has_mig_table BOOLEAN DEFAULT FALSE;
    session_search_path TEXT[];
    search_schema TEXT;
    feature_id INTEGER;
    partial_label TEXT;
    str TEXT;
    sf TEXT;
    report_count INTEGER;
BEGIN
    SELECT EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('migration','subset')) INTO use_staged;
    RAISE INFO 'use_staged is %',  use_staged;
    SELECT STRING_TO_ARRAY(current_setting('search_path'),',') INTO session_search_path;

    FOR search_schema IN SELECT UNNEST(session_search_path) LOOP
        search_schema := BTRIM(search_schema);
        SELECT EXISTS (
           SELECT 1 FROM pg_catalog.pg_class c
           JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
           WHERE  n.nspname = search_schema
           AND    c.relname = 'm_biblio_record_entry'
           AND    c.relkind = 'r'   
          ) INTO has_mig_table;
          RAISE INFO 'has_mig_table for % is %', search_schema, has_mig_table;
          IF has_mig_table THEN 
              EXIT; 
          END IF;
    END LOOP;
    IF has_mig_table AND use_staged THEN
        INSERT INTO exclude_from_batch (record,reason)
            SELECT record, 'has parts' FROM m_biblio_monograph_part WHERE NOT deleted
            AND EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'skip parts' AND value = 'TRUE' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1));
        SELECT COUNT(record)  FROM m_biblio_monograph_part WHERE NOT deleted
             AND EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'skip parts' AND value = 'TRUE' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1))
             INTO report_count;
        RAISE INFO 'exclude based on parts (staged and incumbent) %', report_count;

        INSERT INTO exclude_from_batch (record,reason)
           SELECT DISTINCT acn.record, 'circ mod exclude' FROM m_asset_call_number acn JOIN m_asset_copy acp ON acp.call_number = acn.id WHERE NOT acp.deleted
           AND acp.circ_modifier IN (SELECT BTRIM(value) FROM dedupe_features WHERE name = 'exclude_circ_mod' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1));
        SELECT COUNT(*) FROM exclude_from_batch WHERE reason = 'circ mod exclude' 
           INTO report_count;
        RAISE INFO 'exclude based on circ mod (staged and incumbent) %', report_count;

        -- #potential issue if non migrated copy locations are used as well, uncommong but could happen
        INSERT INTO exclude_from_batch (record,reason)
            SELECT DISTINCT acn.record, 'shelving location exclude' FROM m_asset_call_number acn JOIN m_asset_copy acp ON acp.call_number = acn.id 
            JOIN m_asset_copy_location acl ON acl.id = acp.location
            WHERE NOT acp.deleted
            AND BTRIM(acl.name) IN (SELECT BTRIM(value) FROM dedupe_features WHERE name = 'exclude_copy_location' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1));
        SELECT COUNT(*) FROM exclude_from_batch WHERE reason = 'shelving location exclude'
            INTO report_count;
        RAISE INFO 'exclude based on shelving loc (staged and incumbent) %', report_count; 

        INSERT INTO exclude_from_batch (record,reason)
            SELECT record, 'excluded search format' FROM dedupe_batch WHERE search_format && (SELECT array_agg(value) FROM dedupe_features 
            WHERE name = 'exclude_search_format' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1));
        SELECT COUNT(*) FROM exclude_from_batch WHERE reason = 'excluded search format'
            INTO report_count;
        RAISE INFO 'exclude based on search format (staged and incumbent) %', report_count;

        INSERT INTO exclude_from_batch (record,reason)
            SELECT record, 'no search format' FROM dedupe_batch WHERE search_format IS NULL
            AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'exclude_null_search_format' AND value = 'TRUE' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1));
        SELECT COUNT(*) FROM exclude_from_batch WHERE reason = 'no search format'
            INTO report_count;
        RAISE INFO 'exclude based on no search format (staged and incumbent) %', report_count;

        FOR feature_id IN SELECT id FROM dedupe_features WHERE name = 'exclude_partial_call_number' LOOP
            SELECT value, value2 FROM dedupe_features WHERE id = feature_id INTO partial_label, str;
            INSERT INTO exclude_from_batch (record,reason)
                SELECT DISTINCT acn.record, 'partial call number' FROM m_asset_call_number acn
                WHERE NOT acn.deleted AND acn.label LIKE ('%' || partial_label || '%')
                AND acn.record IN (SELECT record FROM dedupe_batch);
        END LOOP;
        SELECT COUNT(*) FROM exclude_from_batch WHERE reason = 'partial call number' INTO report_count;
        RAISE INFO 'excluded due to partial call number (staged and incument) %', report_count;

        FOR feature_id IN SELECT id FROM dedupe_features WHERE name = 'restrict_sf_match_by_cm' LOOP
            SELECT value, value2 FROM dedupe_features WHERE id = feature_id INTO sf, str;
            INSERT INTO exclude_from_batch (record,reason)
                SELECT DISTINCT acn.record, 'restricted by circ mod' FROM asset.call_number acn JOIN asset.copy acp ON acp.call_number = acn.id WHERE NOT acp.deleted
                AND acp.circ_modifier NOT IN (SELECT value2 FROM dedupe_features WHERE name = 'restrict_sf_match_by_cm' AND value = sf AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1))
                AND acn.record IN (SELECT record FROM dedupe_batch WHERE sf = ANY(search_format));
        END LOOP;
        SELECT COUNT(*) FROM exclude_from_batch WHERE reason = 'restricted by circ mod' INTO report_count;
        RAISE INFO 'excluded due to circ mod restriction (staged and incumbent) %', report_count;
    END IF;
END $$;

/*
-- do we want to apply tag level exclusions to migrated records?  they're a lot iffier.
-- if so we either need a real_full_rec equivalent here or do something with oils_xpathing the recs
DO $$
DECLARE 
    feature_id INTEGER;
    tag_value TEXT;
    str TEXT;
    report_count INTEGER DEFAULT 0;
BEGIN
    FOR feature_id IN SELECT id FROM dedupe_features WHERE name = 'exclude_tag_value' LOOP
        SELECT value, value2 FROM dedupe_features WHERE id = feature_id INTO tag_value, str;
        INSERT INTO exclude_from_batch (record, reason) 
            SELECT DISTINCT record, 'tag exclusion' FROM metabib.real_full_rec WHERE tag = tag_value 
            AND value ~* str AND record IN (SELECT id FROM m_biblio_record_entry_legacy WHERE x_migrate AND x_merge_to IS NULL);
    END LOOP;
    SELECT COUNT(*) FROM exclude_from_batch WHERE reason = 'tag exclusion' INTO report_count;
    RAISE INFO 'excluded due to tag value %', report_count;
END $$;
*/

-- separate do loop for the copy level strings stuff 


DROP TABLE IF EXISTS staged_copy_level_strings;

DO $$
DECLARE
    use_staged BOOLEAN DEFAULT FALSE;
    has_mig_table BOOLEAN DEFAULT FALSE;
    session_search_path TEXT[];
    search_schema TEXT;
    sfs TEXT[];
    feature_id INTEGER;
    sf TEXT;
    str TEXT;
    report_count INTEGER;
BEGIN
    SELECT EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('migration','subset')) INTO use_staged;
    RAISE INFO 'use_staged is %',  use_staged;
    SELECT STRING_TO_ARRAY(current_setting('search_path'),',') INTO session_search_path;

    FOR search_schema IN SELECT UNNEST(session_search_path) LOOP
        search_schema := BTRIM(search_schema);
        SELECT EXISTS (
           SELECT 1 FROM pg_catalog.pg_class c
           JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
           WHERE  n.nspname = search_schema
           AND    c.relname = 'm_biblio_record_entry'
           AND    c.relkind = 'r'
          ) INTO has_mig_table;
          RAISE INFO 'has_mig_table for % is %', search_schema, has_mig_table;
          IF has_mig_table THEN
              EXIT;
          END IF;
    END LOOP;
    IF has_mig_table AND use_staged THEN

        SELECT ARRAY_AGG(DISTINCT value) FROM dedupe_features WHERE name = 'restrict_sf_match_by_string'
        AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1) INTO sfs;

        CREATE UNLOGGED TABLE staged_copy_level_strings AS
        SELECT acn.record, acp.id AS acp_id, CONCAT_WS(' ',acnp.label,acn.label,acns.label) AS label, acl.name AS location
            FROM m_asset_call_number acn
            JOIN m_asset_copy acp ON acp.call_number = acn.id
            JOIN m_asset_copy_location acl ON acl.id = acp.location
            JOIN m_asset_call_number_suffix acns ON acns.id = acn.suffix
            JOIN m_asset_call_number_prefix acnp ON acnp.id = acn.prefix
            WHERE NOT acp.deleted AND acn.record IN (SELECT record FROM dedupe_batch WHERE search_format && sfs);

        ALTER TABLE staged_copy_level_strings ADD COLUMN keep BOOLEAN DEFAULT FALSE;

        FOR feature_id IN SELECT id FROM dedupe_features WHERE name = 'restrict_sf_match_by_string' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1) LOOP
            SELECT value, value2 FROM dedupe_features WHERE id = feature_id INTO sf, str;
            UPDATE staged_copy_level_strings SET keep = TRUE
                WHERE record IN (SELECT record FROM dedupe_batch WHERE sf = ANY(search_format))
                AND (label ~* str OR location ~* str);
        END LOOP;
    END IF;
END $$;

INSERT INTO exclude_from_batch (record,reason) SELECT DISTINCT record, 'restricted by string'
FROM staged_copy_level_strings WHERE NOT keep;

DELETE FROM dedupe_batch WHERE record IN (SELECT DISTINCT record FROM exclude_from_batch);

DROP TABLE staged_copy_level_strings;
