CREATE TABLE gsheet_tracked_column (
    id               SERIAL
    ,table_id        INTEGER REFERENCES gsheet_tracked_table (id)
    ,column_name     TEXT NOT NULL  
);
