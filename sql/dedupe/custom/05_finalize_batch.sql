\timing on

-- vivisect 

UPDATE dedupe_batch SET populated = FALSE WHERE populated = TRUE OR populated IS NULL;

\x off
\t on
\o ~/vivisect_records.sql
SELECT 'SELECT * FROM vivisect_record(' || record || ',''' || (SELECT * FROM get_6xx_scoring_method()) || ''',''production'');' FROM dedupe_batch WHERE staged = FALSE ORDER BY record;
SELECT 'SELECT * FROM vivisect_record(' || record || ',''' || (SELECT * FROM get_6xx_scoring_method()) || ''',''production'');' FROM dedupe_batch WHERE staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value = 'subset') ORDER BY record;
SELECT 'SELECT * FROM vivisect_record(' || record || ',''' || (SELECT * FROM get_6xx_scoring_method()) || ''',''staging'');' FROM dedupe_batch WHERE staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value = 'migration') ORDER BY record;
\o
\t off
\i ~/vivisect_records.sql;

-- if all records are marked as large print in call number or shelving location then make the bib large print
DROP TABLE IF EXISTS bib_acp_lp_map;
CREATE UNLOGGED TABLE bib_acp_lp_map AS 
WITH acl_list AS (SELECT id FROM asset.copy_location WHERE name ~* 'large print' OR name ~* ' lp ' OR name ~* '^lp ' OR name ~* ' lp$')
, acn_list AS (SELECT c.id FROM asset.call_number c 
    JOIN asset.call_number_prefix p on p.id = c.prefix 
    JOIN asset.call_number_suffix s on s.id = c.suffix 
    WHERE CONCAT_WS(' ',p.label,c.label,s.label) ~* 'large print'
    OR CONCAT_WS(' ',p.label,c.label,s.label) ~* ' lp '
    OR CONCAT_WS(' ',p.label,c.label,s.label) ~* '^lp '
    OR CONCAT_WS(' ',p.label,c.label,s.label) ~* ' lp$'
)
SELECT acn.record AS bre_id, acp.id AS acp_id 
FROM asset.copy acp JOIN asset.call_number acn ON acn.id = acp.call_number
WHERE NOT acp.deleted AND (acp.location IN (SELECT id FROM acl_list) OR acp.call_number IN (SELECT id FROM acn_list))
;
ALTER TABLE bib_acp_lp_map ADD COLUMN has_non_lp BOOLEAN DEFAULT FALSE;
UPDATE bib_acp_lp_map SET has_non_lp = TRUE WHERE bre_id IN (
  WITH all_acps AS (
    SELECT acp.id FROM asset.copy acp 
    JOIN asset.call_number acn ON acn.id = acp.call_number 
    WHERE NOT acp.deleted
    AND acn.record IN (SELECT DISTINCT bre_id FROM bib_acp_lp_map)
  )
  SELECT DISTINCT acn.record FROM asset.copy acp 
  JOIN asset.call_number acn ON acn.id = acp.call_number 
  LEFT JOIN bib_acp_lp_map balm ON balm.acp_id = acp.id 
  WHERE NOT acp.deleted
  AND balm.acp_id IS NULL
  AND acn.record IN (SELECT DISTINCT bre_id FROM bib_acp_lp_map)
);
UPDATE dedupe_batch SET search_format = ARRAY_APPEND(search_format,'lpbook') 
	WHERE record IN (SELECT DISTINCT bre_id FROM bib_acp_lp_map WHERE has_non_lp = FALSE)
; 

-- now set lp for migrated records 
DROP TABLE IF EXISTS bib_acp_lp_map;
CREATE UNLOGGED TABLE bib_acp_lp_map AS
WITH acl_list AS (SELECT id FROM m_asset_copy_location WHERE name ~* 'large print' OR name ~* ' lp ' OR name ~* '^lp ' OR name ~* ' lp$')
, acn_list AS (SELECT c.id FROM m_asset_call_number c
    JOIN m_asset_call_number_prefix p on p.id = c.prefix 
    JOIN m_asset_call_number_suffix s on s.id = c.suffix
    WHERE CONCAT_WS(' ',p.label,c.label,s.label) ~* 'large print'
    OR CONCAT_WS(' ',p.label,c.label,s.label) ~* ' lp '
    OR CONCAT_WS(' ',p.label,c.label,s.label) ~* '^lp '
    OR CONCAT_WS(' ',p.label,c.label,s.label) ~* ' lp$'
)
SELECT acn.record AS bre_id, acp.id AS acp_id
FROM m_asset_copy acp JOIN m_asset_call_number acn ON acn.id = acp.call_number
WHERE NOT acp.deleted AND (acp.location IN (SELECT id FROM acl_list) OR acp.call_number IN (SELECT id FROM acn_list))
AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('migration','subset'));
ALTER TABLE bib_acp_lp_map ADD COLUMN has_non_lp BOOLEAN DEFAULT FALSE;
UPDATE bib_acp_lp_map SET has_non_lp = TRUE WHERE bre_id IN (
  WITH all_acps AS (
    SELECT acp.id FROM m_asset_copy acp
    JOIN m_asset_call_number acn ON acn.id = acp.call_number
    WHERE NOT acp.deleted
    AND acn.record IN (SELECT DISTINCT bre_id FROM bib_acp_lp_map)
  )
  SELECT DISTINCT acn.record FROM m_asset_copy acp
  JOIN m_asset_call_number acn ON acn.id = acp.call_number
  LEFT JOIN bib_acp_lp_map balm ON balm.acp_id = acp.id
  WHERE NOT acp.deleted
  AND balm.acp_id IS NULL
  AND acn.record IN (SELECT DISTINCT bre_id FROM bib_acp_lp_map)
)
AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('migration','subset'));
UPDATE dedupe_batch SET search_format = ARRAY_APPEND(search_format,'lpbook')
    WHERE record IN (SELECT DISTINCT bre_id FROM bib_acp_lp_map WHERE has_non_lp = FALSE)
    AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('migration','subset'))
;

UPDATE dedupe_batch SET score_penalty = score_penalty + 10 WHERE search_format IS NULL;
UPDATE dedupe_batch SET score_penalty = score_penalty + 5 WHERE record IN 
	(SELECT DISTINCT record FROM metabib.real_full_rec WHERE tag = '919' AND subfield = 'a' AND value ~* 'modified' AND value ~* 'fixed field');

-- remove various media on request 

UPDATE 
	dedupe_batch 
SET
	derived_search_format = 'cdaudiobook'
WHERE
	EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'derived sf on cdaudiobook' AND value = 'TRUE' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1))
	AND record IN (SELECT DISTINCT record FROM asset.call_number WHERE deleted IS FALSE AND id IN (SELECT call_number FROM asset.copy WHERE deleted IS FALSE 
		AND location IN (SELECT * FROM find_locations_by_names('derived in - cdaudiobook','derived not in - cdaudiobook'))))
	AND search_format IS NULL
;

UPDATE
    dedupe_batch
SET
    derived_search_format = 'blu-ray'
WHERE
    EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'derived sf on blu-ray' AND value = 'TRUE' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1))
    AND record IN (SELECT DISTINCT record FROM asset.call_number WHERE deleted IS FALSE AND id IN (SELECT call_number FROM asset.copy WHERE deleted IS FALSE
        AND location IN (SELECT * FROM find_locations_by_names('derived in - bluray','derived not in - bluray'))))
    AND search_format IS NULL
;

UPDATE
    dedupe_batch
SET
    derived_search_format = 'dvd'
WHERE
    EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'derived sf on dvd' AND value = 'TRUE' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1))
    AND record IN (SELECT DISTINCT record FROM asset.call_number WHERE deleted IS FALSE AND id IN (SELECT call_number FROM asset.copy WHERE deleted IS FALSE
        AND location IN (SELECT * FROM find_locations_by_names('derived in - dvd','derived not in - dvd'))))
    AND search_format IS NULL
;

UPDATE
    dedupe_batch
SET
    derived_search_format = 'book'
WHERE
    EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'derived sf on book' AND value = 'TRUE' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1))
    AND record IN (SELECT DISTINCT record FROM asset.call_number WHERE deleted IS FALSE AND id IN (SELECT call_number FROM asset.copy WHERE deleted IS FALSE
        AND location IN (SELECT * FROM find_locations_by_names('derived in - book','derived not in - book'))))
    AND search_format IS NULL
;

UPDATE dedupe_batch SET search_format = search_format || derived_search_format WHERE derived_search_format IS NOT NULL;
UPDATE dedupe_batch SET all_pubdates = pubdate WHERE pubdate IS NOT NULL;
UPDATE dedupe_batch SET all_pubdates = all_pubdates || rda_pubdate WHERE rda_pubdate IS NOT NULL;
UPDATE dedupe_batch SET all_publishers = publisher WHERE publisher IS NOT NULL;
UPDATE dedupe_batch SET all_publishers = all_publishers || rda_publisher WHERE rda_publisher IS NOT NULL;
UPDATE dedupe_batch SET title = NULL WHERE title = '';
UPDATE dedupe_batch SET subtitle = NULL WHERE subtitle = ''; 
UPDATE dedupe_batch SET titlepart = NULL WHERE titlepart = '';
UPDATE dedupe_batch SET titlepartname = NULL WHERE titlepartname = '';
UPDATE dedupe_batch SET author = NULL WHERE author = '';
UPDATE dedupe_batch SET search_format_str = ARRAY_TO_STRING(search_format,',');
UPDATE dedupe_batch SET avdisc_flag = TRUE WHERE search_format_str IN ('cd','dvd','blu-ray','cdmusic','cdaudiobook');
UPDATE dedupe_batch SET isbn_score = 0 WHERE isbn_values IS NULL OR ARRAY_LENGTH(isbn_values,1) < 1;
UPDATE dedupe_batch SET search_format = NULL WHERE search_format = '{}';
UPDATE dedupe_batch SET content_type = NULL WHERE content_type = '{}';
UPDATE dedupe_batch SET carrier_type = NULL WHERE carrier_type = '{}';
UPDATE dedupe_batch SET media_type = NULL WHERE media_type = '{}';
UPDATE dedupe_batch SET isbn_values = NULL WHERE isbn_values = '{}';
UPDATE dedupe_batch SET upc_values = NULL WHERE upc_values = '{}';
UPDATE dedupe_batch SET issn_values = NULL WHERE issn_values = '{}';
UPDATE dedupe_batch SET all_pubdates = NULL WHERE all_pubdates = '{}';
UPDATE dedupe_batch SET all_publishers = NULL WHERE all_publishers = '{}';
UPDATE dedupe_batch SET edition_terms = NULL WHERE edition_terms = '{}';
UPDATE dedupe_batch SET languages = NULL WHERE languages = '{}';
UPDATE dedupe_batch SET added_entries = NULL WHERE added_entries = '{}';
UPDATE dedupe_batch SET pubdate = NULL WHERE pubdate = '{}';
UPDATE dedupe_batch SET rda_pubdate = NULL WHERE rda_pubdate = '{}';
UPDATE dedupe_batch SET publisher = NULL WHERE publisher = '{}';
UPDATE dedupe_batch SET rda_publisher = NULL WHERE rda_publisher = '{}';
UPDATE dedupe_batch SET oclc_values = NULL WHERE oclc_values = '{}';

UPDATE dedupe_batch SET search_format = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(search_format,'')));
UPDATE dedupe_batch SET content_type = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(content_type,'')));
UPDATE dedupe_batch SET carrier_type = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(carrier_type,'')));
UPDATE dedupe_batch SET media_type = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(media_type,'')));
UPDATE dedupe_batch SET pubdate = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(pubdate,'')));
UPDATE dedupe_batch SET publisher = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(publisher,'')));
UPDATE dedupe_batch SET all_pubdates = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(all_pubdates,'')));
UPDATE dedupe_batch SET all_publishers = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(all_publishers,'')));
UPDATE dedupe_batch SET edition_terms = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(edition_terms,'')));
UPDATE dedupe_batch SET languages = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(languages,'')));
UPDATE dedupe_batch SET added_entries = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(added_entries,'')));
UPDATE dedupe_batch SET oclc_values = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(oclc_values,'')));
UPDATE dedupe_batch SET isbn_values = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(isbn_values,'')));
UPDATE dedupe_batch SET upc_values = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(upc_values,'')));
UPDATE dedupe_batch SET issn_values = ANYARRAY_SORT(ANYARRAY_UNIQ(ARRAY_REMOVE(issn_values,'')));


UPDATE
    dedupe_batch
SET
    title = clean_title(o_title,'primary'),
	subtitle = clean_title(o_subtitle,'sub'),
	titlepart = clean_title(o_titlepart,'part'),
	titlepartname = clean_title(o_titlepartname,'partname'),
    author = clean_author(o_author)
;

SELECT * FROM find_manga_records();

-- remove bibs not wanted as part of the dedupe based on preferences 

DROP TABLE IF EXISTS bib_acp_lp_map;
DROP TABLE IF EXISTS copy_level_strings;
ALTER TABLE dedupe_batch SET LOGGED; 
