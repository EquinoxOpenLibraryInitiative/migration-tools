BEGIN;

DROP TABLE IF EXISTS asset.call_number_temp;
CREATE TABLE asset.call_number_temp AS (
  SELECT * FROM asset.call_number WHERE id = -1
);

DROP TABLE IF EXISTS biblio.record_entry_temp;
CREATE TABLE biblio.record_entry_temp AS (
  SELECT * FROM biblio.record_entry WHERE id = -1
);


TRUNCATE
  action.circulation,
  asset.copy,
  biblio.record_entry,
  asset.call_number,
  metabib.metarecord_source_map,
  metabib.metarecord
CASCADE;

INSERT INTO asset.call_number SELECT * FROM asset.call_number_temp;
INSERT INTO biblio.record_entry SELECT * FROM biblio.record_entry_temp;


