CREATE TABLE gsheet_tracked_table (
    id                          SERIAL
    ,worksheet_name             TEXT NOT NULL
    ,worksheet_key              TEXT
    ,table_name                 TEXT NOT NULL
    ,tab_name                   TEXT
    ,created                    TIMESTAMP
    ,last_pulled                TIMESTAMP
);

