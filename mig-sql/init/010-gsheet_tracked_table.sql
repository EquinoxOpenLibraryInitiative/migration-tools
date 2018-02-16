CREATE TABLE gsheet_tracked_table (
    id                          SERIAL PRIMARY KEY
    ,table_name                 TEXT NOT NULL
    ,tab_name                   TEXT
    ,created                    TIMESTAMP
    ,last_pulled                TIMESTAMP
    ,last_pushed                TIMESTAMP
    ,UNIQUE(table_name,tab_name)
);

