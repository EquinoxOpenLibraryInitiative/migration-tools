DROP TABLE IF EXISTS tracked_column;
CREATE TABLE tracked_column (
     id                 serial
    ,base_filename      TEXT
    ,parent_table       TEXT
    ,staged_table       TEXT
    ,staged_column      TEXT
    ,comment            TEXT
    ,target_table       TEXT
    ,target_column      TEXT
    ,transform          TEXT
    ,summarize          BOOLEAN
);
CREATE INDEX ON tracked_column(target_table,target_column);
CREATE INDEX ON tracked_column(base_filename);
