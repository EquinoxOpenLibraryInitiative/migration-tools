CREATE TABLE map_hold_policies (
    l_user_home_ou      TEXT
    ,l_request_ou       TEXT
    ,l_item_owning_ou   TEXT
    ,l_item_circ_ou     TEXT
    ,l_requestor_grp    TEXT
    ,l_circ_modifier    TEXT
    ,l_active           TEXT
    ,l_holdable         TEXT
    ,l_max_holds        TEXT
    ,l_includes_frozen_holds   TEXT
    ,l_distance_is_from_owner  TEXT
    ,l_transit_range    TEXT
    ,l_usr_grp          TEXT
    ,x_user_home_ou     INTEGER
    ,x_request_ou       INTEGER
    ,x_item_owning_ou   INTEGER
    ,x_item_circ_ou     INTEGER
    ,x_requestor_grp    INTEGER
    ,x_circ_modifier    TEXT
    ,x_active           BOOLEAN
    ,x_holdable         BOOLEAN
    ,x_max_holds        INTEGER
    ,x_includes_frozen_holds   BOOLEAN
    ,x_distance_is_from_owner  BOOLEAN
    ,x_transit_range    INTEGER
    ,x_usr_grp          INTEGER    
   ,x_migrate           BOOLEAN DEFAULT TRUE
);

INSERT INTO gsheet_tracked_table 
    (table_name,tab_name,created)
VALUES
    ('map_hold_policies','Hold Policies',NOW())
;

INSERT INTO gsheet_tracked_column
    (table_id,column_name)
VALUES
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Hold Policies'),'l_user_home_ou')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Hold Policies'),'l_request_ou')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Hold Policies'),'l_item_owning_ou')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Hold Policies'),'l_item_circ_ou')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Hold Policies'),'l_requestor_grp')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Hold Policies'),'l_circ_modifier')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Hold Policies'),'l_active')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Hold Policies'),'l_holdable')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Hold Policies'),'l_max_holds')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Hold Policies'),'l_includes_frozen_holds')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Hold Policies'),'l_distance_is_from_owner')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Hold Policies'),'l_transit_range')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Hold Policies'),'l_usr_grp')
;


CREATE TABLE map_circ_policies (
    l_org_unit              TEXT
    ,l_user_group           TEXT
    ,l_copy_owning_lib      TEXT
    ,l_user_home_lib        TEXT
    ,l_circ_mod             TEXT
    ,l_copy_location        TEXT
    ,l_circulate            TEXT
    ,l_circ_limit_set       TEXT
    ,l_duration_rule        TEXT
    ,l_fine_rule            TEXT
    ,l_grace_override       TEXT
    ,l_max_fine             TEXT
    ,l_notes                TEXT
    ,x_org_unit             INTEGER
    ,x_user_group           INTEGER
    ,x_copy_owning_lib      INTEGER
    ,x_user_home_lib        INTEGER
    ,x_circ_mod             TEXT
    ,x_copy_location        INTEGER
    ,x_circulate            BOOLEAN
    ,x_circ_limit_set       INTEGER
    ,x_duration_rule        INTEGER
    ,x_fine_rule            INTEGER
    ,x_grace_override       INTERVAL
    ,x_max_fine             INTEGER
    ,x_migrate              BOOLEAN DEFAULT FALSE
);

INSERT INTO gsheet_tracked_table
    (table_name,tab_name,created)
VALUES
    ('map_circ_policies','Circ Policies',NOW())
;

INSERT INTO gsheet_tracked_column
    (table_id,column_name)
VALUES
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Circ Policies'),'l_org_unit')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Circ Policies'),'l_user_group')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Circ Policies'),'l_copy_owning')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Circ Policies'),'l_user_home_lib')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Circ Policies'),'l_circ_mod')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Circ Policies'),'l_copy_location')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Circ Policies'),'l_circulate')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Circ Policies'),'l_circ_limit_set')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Circ Policies'),'l_duration_rule')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Circ Policies'),'l_fine_rule')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Circ Policies'),'l_grace_override')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Circ Policies'),'l_max_fine')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Circ Policies'),'l_notes')
;

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


CREATE TABLE map_bib_manipulations (
    id               SERIAL
    ,action          TEXT
    ,field           TEXT
    ,subfield        TEXT
    ,matching_value  TEXT
    ,target_value    TEXT
    ,note            TEXT
);

INSERT INTO gsheet_tracked_table
    (table_name,tab_name,created)
VALUES
    ('map_bib_manipulations','Bib Records',NOW())
;

INSERT INTO gsheet_tracked_column
    (table_id,column_name)
VALUES
     ((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Bib Records'),'name')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Bib Records'),'action')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Bib Records'),'field')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Bib Records'),'subfield')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Bib Records'),'matching_value')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Bib Records'),'target_value')
    ,((SELECT id FROM gsheet_tracked_table WHERE tab_name = 'Bib Records'),'note')
;


