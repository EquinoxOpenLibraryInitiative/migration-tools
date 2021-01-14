DROP TABLE IF EXISTS groups;
CREATE TABLE groups (id SERIAL, records BIGINT[], pairs INTEGER [], match_set TEXT, merge_sets TEXT[],
								lead_record BIGINT, lead_selected BOOLEAN DEFAULT FALSE, score INTEGER, done BOOLEAN DEFAULT FALSE);
CREATE INDEX dedupe_groups_records_x ON groups USING GIN (records);
CREATE INDEX dedupe_groups_match_set_x ON groups (match_set);

\x off
\t on
\o ~/group_pairs.sql
SELECT 'SELECT * FROM group_pairs(' || id || ');' FROM pairs ORDER BY id;
\o
\t off
\i ~/group_pairs.sql;

DO $$
DECLARE 
   group_limit INTEGER;
   deleted_count INTEGER;
BEGIN
   SELECT dedupe_setting('merge_group_limit') INTO group_limit;
    IF group_limit IS NOT NULL THEN  
       INSERT INTO exclude_from_batch (record,reason) SELECT UNNEST(records), 'group exceeds limit' FROM groups 
       WHERE ARRAY_LENGTH(records,1) > group_limit;
       DELETE FROM groups WHERE ARRAY_LENGTH(records,1) > group_limit RETURNING * INTO deleted_count;
       RAISE INFO 'groups deleted is %', deleted_count;
    END IF;
END $$;

\x off
\t on
\o ~/find_lead_record.sql
SELECT 'SELECT * FROM find_lead_record(' || id || ',''' || (SELECT value FROM dedupe_features WHERE name = 'dedupe_type') || ''');' FROM groups ORDER BY id;
\o
\t off
\i ~/find_lead_record.sql;

-- had some recent trouble with ARRAY_REMOVE working consistently so a sanity check
UPDATE groups SET records = ARRAY_REMOVE(records,lead_record);

DROP TABLE IF EXISTS pairs;
DROP TABLE IF EXISTS incoming_bibs;
DROP TABLE IF EXISTS phys_desc;
DROP TABLE IF EXISTS subjects;

