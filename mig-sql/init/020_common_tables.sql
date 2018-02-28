CREATE TABLE map_create_shelving_location (
    l_id                  SERIAL
    ,l_owning_lib         TEXT            
    ,l_copy_location      TEXT
    ,l_opac_visible       TEXT
    ,l_checkin_alert      TEXT
    ,l_holdable           TEXT
    ,l_circulate          TEXT
    ,l_note               TEXT
    ,x_migrate            BOOLEAN NOT NULL DEFAULT TRUE
    ,x_shelf              INTEGER
) INHERITS (asset_copy_location);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_create_shelving_location','New Copy Locations',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Copy Locations'),'l_owning_lib')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Copy Locations'),'l_copy_location')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Copy Locations'),'l_opac_visible')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Copy Locations'),'l_checkin_alert')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Copy Locations'),'l_holdable')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Copy Locations'),'l_circulate')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Copy Locations'),'l_note')
;

CREATE TABLE map_create_account (
    l_id                  SERIAL
    ,l_usrname            TEXT            
    ,l_first_name         TEXT
    ,l_family_name        TEXT
    ,l_email              TEXT
    ,l_password           TEXT
    ,l_home_library       TEXT
    ,l_profile1           TEXT
    ,l_profile2           TEXT
    ,l_profile3           TEXT
    ,l_work_ou            TEXT
    ,l_note               TEXT
    ,l_note2              TEXT
    ,x_migrate            BOOLEAN NOT NULL DEFAULT TRUE
) INHERITS (actor_usr);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_create_account','New Accounts',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'l_usrname')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'l_first_name')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'l_family_name')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'l_email')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'l_password')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'l_home_library')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'l_profile1')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'l_profile2')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'l_profile3')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'l_work_ou')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'l_note')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'New Accounts'),'l_note2')
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
    ,count       TEXT            
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
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Miscellaneous Options'),'count')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Miscellaneous Options'),'option')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Miscellaneous Options'),'Choice')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Miscellaneous Options'),'value')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Miscellaneous Options'),'note')
;

CREATE TABLE map_org_setting (
    l_id             SERIAL
    ,l_name          TEXT            
    ,l_label         TEXT
    ,l_entry_type    TEXT
    ,l_org_unit      TEXT
    ,l_value         TEXT
    ,l_note          TEXT
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES 
    ('map_org_setting','Org Settings',NOW())
;

INSERT INTO gsheet_tracked_column 
    (table_id,column_name) 
VALUES 
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Org Settings'),'l_name')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Org Settings'),'l_label')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Org Settings'),'l_entry_type')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Org Settings'),'l_org_unit')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Org Settings'),'l_value')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Org Settings'),'l_note')
;
