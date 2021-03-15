\timing on

DROP TABLE IF EXISTS incumbent_titles;
DROP TABLE IF EXISTS incoming_titles;

-- these basically save time by not vivisecting titles that will never be compared
CREATE UNLOGGED TABLE incumbent_titles AS 
    SELECT record, value, clean_title(value,'primary') AS clean_title
    FROM metabib.real_full_rec 
    WHERE record IN (SELECT id FROM biblio.record_entry WHERE deleted = FALSE) AND tag = '245' AND subfield = 'a'
    AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('migration','subset'));

DO $$
DECLARE 
    row_count INTEGER DEFAULT 0;
BEGIN
    IF EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('migration','subset')) THEN
    CREATE UNLOGGED TABLE incoming_titles AS
        SELECT id AS record, UNNEST(oils_xpath( '//*[@tag="245"]/*[@code="a"]/text()', marc)) AS value,
        clean_title(UNNEST(oils_xpath( '//*[@tag="245"]/*[@code="a"]/text()', marc)),'primary') AS clean_title
        FROM m_biblio_record_entry;
    SELECT COUNT(*) FROM incoming_titles INTO row_count;
    END IF;
    RAISE NOTICE 'rows in incoming_titles : %', row_count;
END $$;

DO $$
DECLARE 
    row_count INTEGER DEFAULT 0;
BEGIN
    IF EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value = 'subset') THEN
        SELECT COUNT(*) FROM incumbent_titles WHERE record IN (SELECT record FROM incoming_titles) INTO row_count;
        DELETE FROM incumbent_titles WHERE record IN (SELECT record FROM incoming_titles); 
	END IF;
    RAISE NOTICE 'rows deleted : %', row_count;
END $$;

DROP TABLE IF EXISTS dedupe_batch;
CREATE UNLOGGED TABLE dedupe_batch (
		record INTEGER, can_have_copies BOOLEAN DEFAULT TRUE, 
		populated BOOLEAN DEFAULT FALSE,  
		isbn_values TEXT[], upc_values TEXT[], issn_values TEXT[], avdisc_flag BOOLEAN DEFAULT FALSE,
		search_format_str TEXT,
        title TEXT, o_title TEXT, subtitle TEXT, o_subtitle TEXT, author TEXT, o_author TEXT,
		titlepart TEXT, o_titlepart TEXT, titlepartname TEXT, o_titlepartname TEXT,
		pubdate TEXT[], rda_pubdate TEXT[], all_pubdates TEXT[],  
		publisher TEXT[], rda_publisher TEXT[], all_publishers TEXT[], manga BOOLEAN DEFAULT FALSE, 
		search_format TEXT[], content_type TEXT[], media_type TEXT[], carrier_type TEXT[], oclc_values TEXT[], 
		derived_search_format TEXT, description TEXT,
    	subject_score INTEGER, isbn_score INTEGER, issn_score INTEGER, ost_score INTEGER,
    	linked_entry_score INTEGER, added_entry_score INTEGER, authority_score INTEGER,
    	description_score INTEGER, author_score INTEGER, edition_score INTEGER,
    	title_score INTEGER, heading_score INTEGER,
    	score INTEGER, score_bonus INTEGER DEFAULT 0, score_penalty INTEGER DEFAULT 0,
		edition_terms TEXT[], languages TEXT[], added_entries TEXT[],
        staged BOOLEAN DEFAULT FALSE
);
CREATE INDEX dedupe_batch_recordx ON dedupe_batch (record);
CREATE INDEX dedupe_batch_titlex ON dedupe_batch (title);
CREATE INDEX dedupe_batch_authorx ON dedupe_batch (author);
CREATE INDEX dedupe_batch_subtitlex ON dedupe_batch (subtitle);
CREATE INDEX dedupe_batch_oclcsx ON dedupe_batch USING GIN (oclc_values);
CREATE INDEX dedupe_batch_issnsx ON dedupe_batch USING GIN (issn_values);
CREATE INDEX dedupe_batch_isbnsx ON dedupe_batch USING GIN (isbn_values);
CREATE INDEX dedupe_batch_upcsx ON dedupe_batch USING GIN (upc_values);
CREATE INDEX dedupe_batch_search_formatx ON dedupe_batch USING GIN (search_format);
CREATE INDEX dedupe_batch_content_typex ON dedupe_batch USING GIN (content_type);
CREATE INDEX dedupe_batch_media_typex ON dedupe_batch USING GIN (media_type);
CREATE INDEX dedupe_batch_carrier_typex ON dedupe_batch USING GIN (carrier_type);
CREATE INDEX dedupe_batch_pubdatesx ON dedupe_batch USING GIN (all_pubdates);
CREATE INDEX dedupe_batch_publishersx ON dedupe_batch USING GIN (all_publishers);
CREATE INDEX dedupe_batch_search_format_str ON dedupe_batch (search_format_str);
CREATE INDEX dedupe_batch_title_partx ON dedupe_batch (titlepart);
CREATE INDEX dedupe_batch_title_part_namex ON dedupe_batch (titlepartname);

TRUNCATE dedupe_batch;

DO $$
DECLARE 
    row_count INTEGER DEFAULT 0;
BEGIN
    IF EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('migration','subset')) THEN
    WITH bib_list AS (
        SELECT DISTINCT record FROM incumbent_titles
        WHERE clean_title IN (SELECT DISTINCT clean_title FROM incoming_titles)
        AND record != -1
          UNION ALL
        SELECT id AS record FROM m_biblio_record_entry WHERE deleted = FALSE
    )
    INSERT INTO dedupe_batch (record) SELECT record FROM bib_list;
    SELECT COUNT(*) FROM dedupe_batch INTO row_count;
    END IF;
    RAISE NOTICE 'rows added to dedupe_batch from incumbents : %', row_count;
END $$;

INSERT INTO dedupe_batch (record)
SELECT id FROM biblio.record_entry WHERE NOT deleted AND id > 0
AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value = 'inclusive')
;


DO $$
DECLARE
    row_count INTEGER DEFAULT 0;
BEGIN
    IF EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('migration','subset')) THEN
        SELECT COUNT(*) FROM dedupe_batch WHERE record IN (SELECT id FROM m_biblio_record_entry) INTO row_count;
        UPDATE dedupe_batch SET staged = TRUE WHERE record IN (SELECT id FROM m_biblio_record_entry);
    END IF;
    RAISE NOTICE 'records marked staged : %', row_count;
END $$;

UPDATE dedupe_batch SET can_have_copies = FALSE WHERE record IN 
  (
    SELECT id FROM biblio.record_entry WHERE source IN 
	(SELECT id FROM config.bib_source WHERE can_have_copies = FALSE)
  )
;

DROP TABLE IF EXISTS incumbent_titles;
DROP TABLE IF EXISTS incoming_titles;

-- assign attributes so we can remove some items 
UPDATE dedupe_batch SET populated = FALSE WHERE populated = TRUE OR populated IS NULL;
\x off
\t on
\o ~/assign_attributes.sql
SELECT 'SELECT ' || record || ' FROM assign_attributes(' || record || ',''production'');' FROM dedupe_batch WHERE staged = FALSE ORDER BY record;
SELECT 'SELECT ' || record || ' FROM assign_attributes(' || record || ',''production'');' FROM dedupe_batch WHERE staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value = 'subset') ORDER BY record;
SELECT 'SELECT ' || record || ' FROM assign_attributes(' || record || ',''staging'');' FROM dedupe_batch WHERE staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value = 'migration') ORDER BY record;
\o
\t off
\i ~/assign_attributes.sql;
