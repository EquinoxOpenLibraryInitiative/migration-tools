DROP TABLE IF EXISTS m_tracked_file;
CREATE TABLE m_tracked_file (
     id                 serial
    ,base_filename      VARCHAR(80) UNIQUE
    ,has_headers        BOOLEAN
    ,headers_file       TEXT
    ,utf8_filename      TEXT
    ,clean_filename     TEXT
    ,stage_sql_filename TEXT
    ,map_sql_filename   TEXT
    ,prod_sql_filename  TEXT
    ,parent_table       TEXT
    ,staged_table       TEXT
);
