CREATE TABLE gsheet_tracked_table (
    id                          SERIAL PRIMARY KEY
    ,sheet_name                 TEXT NOT NULL
    ,table_name                 TEXT NOT NULL
    ,tab_name                   TEXT
    ,created                    TIMESTAMP
    ,last_pulled                TIMESTAMP
    ,last_pushed                TIMESTAMP
);

CREATE TABLE gsheet_tracked_column (
    id               SERIAL
    ,table_id        INTEGER REFERENCES gsheet_tracked_table (id)
    ,column_name     TEXT NOT NULL  
);
