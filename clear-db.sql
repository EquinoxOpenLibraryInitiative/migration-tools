SET CLIENT_ENCODING TO 'UNICODE';

BEGIN;

SELECT reporter.disable_materialized_simple_record_trigger();

CREATE TEMP TABLE dummy_bib AS SELECT * FROM biblio.record_entry WHERE id = -1;
CREATE TEMP TABLE dummy_cn AS SELECT * FROM asset.call_number WHERE id = -1;

CREATE TEMP TABLE backup_loc AS SELECT * FROM asset.copy_location WHERE id = 1;
CREATE TEMP TABLE backup_usr AS SELECT * FROM actor.usr WHERE id = 1;
CREATE TEMP TABLE backup_card AS SELECT * FROM actor.card WHERE usr = 1;

UPDATE backup_usr SET card = NULL;

DROP INDEX metabib.metabib_full_rec_index_vector_idx;
DROP INDEX metabib.metabib_full_value_idx;
DROP INDEX metabib.metabib_title_field_entry_index_vector_idx;
DROP INDEX metabib.metabib_author_field_entry_index_vector_idx;
DROP INDEX metabib.metabib_subject_field_entry_index_vector_idx;
DROP INDEX metabib.metabib_keyword_field_entry_index_vector_idx;
DROP INDEX metabib.metabib_series_field_entry_index_vector_idx;
DROP INDEX metabib.metabib_full_rec_tag_subfield_idx;


TRUNCATE biblio.record_entry CASCADE;
TRUNCATE metabib.full_rec;
TRUNCATE metabib.rec_descriptor;
TRUNCATE metabib.title_field_entry;
TRUNCATE metabib.author_field_entry;
TRUNCATE metabib.subject_field_entry;
TRUNCATE metabib.keyword_field_entry;
TRUNCATE metabib.series_field_entry;
TRUNCATE auditor.biblio_record_entry_history;
TRUNCATE asset.copy_location CASCADE;

TRUNCATE actor.usr CASCADE;
TRUNCATE actor.card CASCADE;
TRUNCATE actor.usr_address CASCADE;
TRUNCATE actor.stat_cat CASCADE;
TRUNCATE actor.stat_cat_entry_usr_map CASCADE;
TRUNCATE money.grocery CASCADE;
TRUNCATE money.billing CASCADE;
TRUNCATE action.circulation CASCADE;
TRUNCATE action.hold_request CASCADE;

SELECT SETVAL('biblio.record_entry_id_seq', 1);

SELECT SETVAL('money.billable_xact_id_seq', 1);
SELECT SETVAL('money.billing_id_seq', 1);

SELECT SETVAL('action.hold_request_id_seq', 1);

SELECT SETVAL('asset.call_number_id_seq', 2);
SELECT SETVAL('asset.copy_id_seq', 1);
SELECT SETVAL('asset.copy_location_id_seq', 2);
SELECT SETVAL('asset.stat_cat_id_seq', 1);
SELECT SETVAL('asset.stat_cat_entry_id_seq', 1);
SELECT SETVAL('asset.stat_cat_entry_copy_map_id_seq', 1);

SELECT SETVAL('actor.usr_id_seq', 2);
SELECT SETVAL('actor.card_id_seq', 2);
SELECT SETVAL('actor.usr_address_id_seq', 2);
SELECT SETVAL('actor.stat_cat_id_seq', 1);
SELECT SETVAL('actor.stat_cat_entry_id_seq', 1);
SELECT SETVAL('actor.stat_cat_entry_usr_map_id_seq', 1);

SELECT SETVAL('metabib.full_rec_id_seq', 1);
SELECT SETVAL('metabib.rec_descriptor_id_seq', 1);
SELECT SETVAL('metabib.title_field_entry_id_seq', 1);
SELECT SETVAL('metabib.author_field_entry_id_seq', 1);
SELECT SETVAL('metabib.subject_field_entry_id_seq', 1);
SELECT SETVAL('metabib.keyword_field_entry_id_seq', 1);
SELECT SETVAL('metabib.series_field_entry_id_seq', 1);
SELECT SETVAL('metabib.metarecord_id_seq', 1);
SELECT SETVAL('metabib.metarecord_source_map_id_seq', 1);

INSERT INTO biblio.record_entry SELECT * FROM dummy_bib;
INSERT INTO asset.call_number SELECT * FROM dummy_cn;
INSERT INTO asset.copy_location SELECT * FROM backup_loc;
INSERT INTO actor.usr SELECT * FROM backup_usr;
INSERT INTO actor.card SELECT * FROM backup_card;
UPDATE actor.usr SET card = actor.card.id FROM actor.card WHERE actor.usr.id = actor.card.usr;
SELECT SETVAL('actor.usr_id_seq', (SELECT MAX(id)+1 FROM actor.usr));
SELECT SETVAL('actor.card_id_seq', (SELECT MAX(id)+1 FROM actor.card));

-- Put any scripts that reload bibs/items/etc here.  Example included.
/*  

\i incumbent.sql
\i incoming.sql

*/
\i IN.sql

CREATE INDEX metabib_title_field_entry_index_vector_idx ON metabib.title_field_entry USING GIST (index_vector);
CREATE INDEX metabib_author_field_entry_index_vector_idx ON metabib.author_field_entry USING GIST (index_vector);
CREATE INDEX metabib_subject_field_entry_index_vector_idx ON metabib.subject_field_entry USING GIST (index_vector);
CREATE INDEX metabib_keyword_field_entry_index_vector_idx ON metabib.keyword_field_entry USING GIST (index_vector);
CREATE INDEX metabib_series_field_entry_index_vector_idx ON metabib.series_field_entry USING GIST (index_vector);
CREATE INDEX metabib_full_rec_index_vector_idx ON metabib.full_rec USING GIST (index_vector);
CREATE INDEX metabib_full_rec_tag_subfield_idx ON metabib.full_rec (tag,subfield);
CREATE INDEX metabib_full_value_idx ON metabib.full_rec (value);

/*  Run the AFTER committing ...

ALTER TABLE metabib.metarecord_source_map DROP CONSTRAINT metabib_metarecord_source_map_metarecord_fkey;

TRUNCATE metabib.metarecord;
TRUNCATE metabib.metarecord_source_map;

INSERT INTO metabib.metarecord (fingerprint,master_record)
    SELECT  fingerprint,id
      FROM  (SELECT DISTINCT ON (fingerprint)
            fingerprint, id, quality
          FROM  biblio.record_entry
          ORDER BY fingerprint, quality desc) AS x
      WHERE fingerprint IS NOT NULL;

INSERT INTO metabib.metarecord_source_map (metarecord,source)
    SELECT  m.id, b.id
      FROM  biblio.record_entry b
        JOIN metabib.metarecord m ON (m.fingerprint = b.fingerprint);

ALTER TABLE metabib.metarecord_source_map
	ADD CONSTRAINT metabib_metarecord_source_map_metarecord_fkey
		FOREIGN KEY (metarecord) REFERENCES metabib.metarecord (id) DEFERRABLE INITIALLY DEFERRED;

*/


/* And this too, if it's production

SELECT reporter.enable_materialized_simple_record_trigger();

*/

-- COMMIT;
-- VACUUM FULL;

