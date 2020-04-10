DROP TABLE IF EXISTS m_tracked_column;
CREATE TABLE m_tracked_column (
     id                 serial
    ,base_filename      VARCHAR(80)
    ,parent_table       TEXT
    ,staged_table       TEXT
    ,staged_column      TEXT
    ,comment            TEXT
    ,target_table       VARCHAR(80)
    ,target_column      VARCHAR(80)
    ,transform          TEXT
    ,summarize          BOOLEAN
);
CREATE INDEX targets_idx ON m_tracked_column(target_table,target_column);
CREATE INDEX base_filename_idx ON m_tracked_column(base_filename);
