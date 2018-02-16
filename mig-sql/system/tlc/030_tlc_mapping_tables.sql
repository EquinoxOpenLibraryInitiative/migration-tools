CREATE TABLE map_tlc_perm_group (
    id                  SERIAL
    ,x_count            TEXT            
    ,legacy_group       TEXT
    ,target_group       TEXT
    ,stat_cat_name      TEXT
    ,stat_cat_entry     TEXT
    ,dnm                TEXT
    ,note               TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_tlc_perm_group','Patron Type',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Type'),'x_count')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Type'),'legacy_group')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Type'),'target_group')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Type'),'stat_cat_name')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Type'),'stat_cat_entry')    
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Type'),'dmn')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Type'),'note')
;

-- ############################################

CREATE TABLE map_tlc_patron_expire (
    id                  SERIAL
    ,x_count            TEXT            
    ,expire_year        TEXT
    ,set_to_date        TEXT
    ,dnm                TEXT
    ,note               TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_tlc_patron_expire','Patrons by Expiration Date',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Expiration Date'),'x_count')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Expiration Date'),'expire_year')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Expiration Date'),'set_to_date')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Expiration Date'),'dnm')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Expiration Date'),'note')
;
 
-- ############################################

CREATE TABLE map_tlc_patron_last_active (
    id                  SERIAL
    ,x_count            TEXT            
    ,last_active        TEXT
    ,inactive           TEXT
    ,dnm                TEXT
    ,note               TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_tlc_patron_last_active','Patrons by Last Active Date',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Last Active Date'),'x_count')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Last Active Date'),'last_active')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Last Active Date'),'inactive')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Last Active Date'),'dnm')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Last Active Date'),'note')
;

-- ############################################

CREATE TABLE map_tlc_billing_type (
    id                  SERIAL
    ,x_count            TEXT            
    ,billing_type       TEXT
    ,dnm                TEXT
    ,note               TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_tlc_billing_type','Migrating Bills by Bill Type',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Migrating Bills by Bill Type'),'x_count')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Migrating Bills by Bill Type'),'billing_type')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Migrating Bills by Bill Type'),'dnm')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Migrating Bills by Bill Type'),'note')
;

-- ############################################

CREATE TABLE map_tlc_password (
    id                  SERIAL
    ,x_count            TEXT            
    ,note               TEXT
    ,migrate_available  TEXT
    ,fill_in_method     TEXT
    ,static_value       TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_tlc_password','Patrons w NULL Passwords',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons w NULL Passwords'),'x_count')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons w NULL Passwords'),'note')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons w NULL Passwords'),'migrate_available')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons w NULL Passwords'),'fill_in_method')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons w NULL Passwords'),'static_value')
;

-- ############################################

CREATE TABLE map_tlc_phone (
    id                  SERIAL
    ,x_count            TEXT            
    ,note               TEXT
    ,migrate_available  TEXT
    ,fill_in_method     TEXT
    ,static_value       TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_tlc_phone','Patron Phone Numbers',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Phone Numbers'),'x_count')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Phone Numbers'),'legacy_phone')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Phone Numbers'),'evergreen_phone')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Phone Numbers'),'note')
;

-- ############################################

CREATE TABLE map_tlc_block_status (
    id                  SERIAL
    ,x_count            TEXT            
    ,tlc_block_status   TEXT
    ,block              TEXT
    ,bar                TEXT
    ,dnm                TEXT
    ,note               TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_tlc_block_status','Patrons by Block Status',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Block Status'),'x_count')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Block Status'),'tlc_block_status')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Block Status'),'block')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Block Status'),'bar')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Block Status'),'dnm')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Block Status'),'note')
;


-- ############################################

CREATE TABLE map_tlc_patron_gender (
    id                  SERIAL
    ,x_count            TEXT            
    ,tlc_block_status   TEXT
    ,block              TEXT
    ,bar                TEXT
    ,dnm                TEXT
    ,note               TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_tlc_patron_gender','Patrons by Gender',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Gender'),'x_count')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Gender'),'tlc_block_status')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Gender'),'block')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Gender'),'bar')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Gender'),'dnm')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patrons by Gender'),'note')
;


-- ############################################


CREATE TABLE map_tlc_holding_code (
    id                  SERIAL
    ,x_count            TEXT            
    ,holding_code       TEXT
    ,shelving_location  TEXT
    ,circ_mod           TEXT
    ,alert              TEXT
    ,alert_message      TEXT
    ,dnm                TEXT
    ,note               TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_tlc_holding_code','Holdings Code',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Holdings Code'),'x_count')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Holdings Code'),'holding_code')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Holdings Code'),'shelving_location')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Holdings Code'),'circ_mod')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Holdings Code'),'alert')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Holdings Code'),'alert_message')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Holdings Code'),'dnm')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Holdings Code'),'note')
;




-- ############################################


CREATE TABLE map_tlc_stat_cat (
    id                  SERIAL
    ,x_count            TEXT            
    ,tlc_stat_cat       TEXT
    ,stat_cat           TEXT
    ,stat_cat_entry     TEXT
    ,show               TEXT
    ,required           TEXT
    ,dnm                TEXT
    ,note               TEXT
    ,note2              TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_tlc_stat_cat','Patron Stat Cats',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Stat Cats'),'x_count')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Stat Cats'),'tlc_stat_cat')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Stat Cats'),'stat_cat')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Stat Cats'),'stat_cat_entry')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Stat Cats'),'show')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Stat Cats'),'required')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Stat Cats'),'dnm')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Stat Cats'),'note')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Stat Cats'),'note2')
;


-- ############################################

CREATE TABLE map_tlc_patron_note (
    id                  SERIAL
    ,x_count            TEXT            
    ,legacy_mnote_type  TEXT
    ,exception          TEXT
    ,action             TEXT
    ,note               TEXT
    ,circulate          TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_tlc_patron_note','Patron Notes',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Notes'),'x_count')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Notes'),'note_type')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Notes'),'subset_values')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Notes'),'matching_text')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Notes'),'action')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Notes'),'note')
;

-- ############################################

CREATE TABLE map_tlc_item_note (
    id             SERIAL
    ,x_count       TEXT            
    ,note_type     TEXT
    ,subset_values TEXT
    ,matching_text TEXT
    ,action        TEXT
    ,note          TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_tlc_item_note','Item Notes',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Item Notes'),'x_count')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Item Notes'),'note_type')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Item Notes'),'subset_values')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Item Notes'),'matching_text')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Item Notes'),'action')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Item Notes'),'note')
;


