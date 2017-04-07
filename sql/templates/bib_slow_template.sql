SET CLIENT_ENCODING TO 'UNICODE';

BEGIN;

UPDATE
        config.internal_flag
SET
        enabled = 't'
WHERE
        name IN ('ingest.assume_inserts_only','ingest.disable_located_uri','ingest.metarecord_mapping.skip_on_insert');

\COPY biblio.record_entry (active,create_date,creator,deleted,edit_date,editor,fingerprint,id,last_xact_id,marc,quality,source,tcn_source,tcn_value,owner,share_depth) FROM 'XXX'

UPDATE
        config.internal_flag
SET
        enabled = 'f'
WHERE
        name IN ('ingest.assume_inserts_only','ingest.disable_located_uri','ingest.metarecord_mapping.skip_on_insert');

COMMIT;
