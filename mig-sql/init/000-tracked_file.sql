DROP TABLE IF EXISTS tracked_file;
CREATE TABLE tracked_file (
     id                 serial
    ,base_filename      TEXT UNIQUE
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
