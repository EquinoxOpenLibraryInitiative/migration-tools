DROP TABLE IF EXISTS pairs;
CREATE TABLE pairs (
    id SERIAL
    ,match_set TEXT
    ,merge_set TEXT
    ,records BIGINT[]
    ,grouped BOOLEAN DEFAULT FALSE
);

CREATE INDEX dedupe_pairs_id_x ON pairs (id);
CREATE INDEX dedupe_pairs_records_x ON pairs USING GIN (records);
CREATE INDEX dedupe_pairs_match_set_x ON pairs (match_set);


