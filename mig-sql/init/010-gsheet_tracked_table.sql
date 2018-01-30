CREATE TABLE gsheet_tracked_table (
    id                          SERIAL PRIMARY KEY
    ,worksheet_name             TEXT NOT NULL
    ,worksheet_key              TEXT
    ,table_name                 TEXT NOT NULL
    ,tab_name                   TEXT
    ,created                    TIMESTAMP
    ,last_pulled                TIMESTAMP
    ,last_pushed                TIMESTAMP
    ,UNIQUE(worksheet_name,tab_name)
);

