\timing on

/*
if x_merge_to isn't on m_biblio_record_entry_legacy then migration-tools should be updated
mig stage-bibs adds it by default

if have have to add it ...
ALTER TABLE m_biblio_record_entry_legacy ADD COLUMN x_merge_to BIGINT;

if this is being done for staged records there is no actual merging to do, just new targets for the bibs 
*/

WITH tmp AS (SELECT lead_record, UNNEST(records) AS record FROM groups)
UPDATE m_biblio_record_entry_legacy a
SET x_merge_to = t.lead_record,
    x_migrate = FALSE,
    x_migrate_reason = ARRAY_APPEND(x_migrate_reason,'deduplicated record')
FROM tmp t WHERE t.record = a.id;
