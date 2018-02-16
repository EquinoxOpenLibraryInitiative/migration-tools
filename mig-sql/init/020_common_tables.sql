CREATE TABLE map_create_shelving_location (
    id                  SERIAL
    ,owning_lib         TEXT            
    ,location_name      TEXT
    ,opac_visible       TEXT
    ,checkin_alert      TEXT
    ,holdable           TEXT
    ,circulate          TEXT
    ,note               TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_create_shelving_location','New Copy Locations',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Copy Locations'),'owning_lib')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Copy Locations'),'location_name')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Copy Locations'),'opac_visible')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Copy Locations'),'checkin_alert')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Copy Locations'),'holdable')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Copy Locations'),'circulate')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Copy Locations'),'note')
;

CREATE TABLE map_create_account (
    id                  SERIAL
    ,usrname            TEXT            
    ,first_name         TEXT
    ,family_name        TEXT
    ,email              TEXT
    ,password           TEXT
    ,home_library       TEXT
    ,profile1           TEXT
    ,profile2           TEXT
    ,profile3           TEXT
    ,work_ou            TEXT
    ,note               TEXT
    ,note2              TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_create_account','New Accounts',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'usrname')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'first_name')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'family_name')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'email')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'password')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'home_library')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'profile1')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'profile2')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'profile3')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'work_ou')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'note')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'note2')
;


CREATE TABLE map_threshold (
    id                  SERIAL
    ,profile            TEXT            
    ,checkout_threshold TEXT
    ,fine_threshold     TEXT
    ,overdue_threshold  TEXT
    ,note               TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_threshold','Patron Thresholds',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Thresholds'),'profile')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Thresholds'),'checkout_threshold')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Thresholds'),'fine_threshold')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Thresholds'),'overdue_threshold')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Patron Thresholds'),'note')
;


CREATE TABLE map_misc (
    id             SERIAL
    ,x_count       TEXT            
    ,option        TEXT
    ,choice        TEXT
    ,value         TEXT
    ,note          TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_misc','Miscellaneous Options',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Miscellaneous Options'),'x_count')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Miscellaneous Options'),'option')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Miscellaneous Options'),'Choice')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Miscellaneous Options'),'value')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Miscellaneous Options'),'note')
;

CREATE TABLE map_org_setting (
    id             SERIAL
    ,name          TEXT            
    ,label         TEXT
    ,entry_type    TEXT
    ,org_unit      TEXT
    ,value         TEXT
    ,note          TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_org_setting','Org Settings',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Org Settings'),'name')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Org Settings'),'label')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Org Settings'),'entry_type')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Org Settings'),'org_unit')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Org Settings'),'value')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Org Settings'),'note')
;
