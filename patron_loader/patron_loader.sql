-- for use if you want to setup the tables in advance especailly to do mapping 

CREATE SCHEMA patron_loader;

CREATE TABLE patron_loader.header (id SERIAL, org_unit TEXT, import_header TEXT, default_header TEXT);

CREATE TABLE patron_loader.log (id SERIAL, session BIGINT, event TEXT, record_count INTEGER, logtime TIMESTAMP DEFAULT NOW());

CREATE TABLE patron_loader.mapping (id SERIAL, org_unit TEXT, mapping_type TEXT, import_value TEXT, native_value TEXT);

CREATE OR REPLACE FUNCTION patron_loader.set_salted_passwd(INTEGER,TEXT) RETURNS BOOLEAN AS $$
    DECLARE
        usr_id              ALIAS FOR $1;
        plain_passwd        ALIAS FOR $2;
        plain_salt          TEXT;
        md5_passwd          TEXT;
    BEGIN
        SELECT actor.create_salt('main') INTO plain_salt;
        SELECT MD5(plain_passwd) INTO md5_passwd;
        PERFORM actor.set_passwd(usr_id, 'main', MD5(plain_salt || md5_passwd), plain_salt);
        RETURN TRUE;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;


/* examples of mapping


INSERT INTO patron_loader.mapping (org_unit,mapping_type,import_value,native_value) VALUES ('SYS1','profile','01','Elementary');
INSERT INTO patron_loader.mapping (org_unit,mapping_type,import_value,native_value) VALUES ('SYS1','profile','02','Elementary');
INSERT INTO patron_loader.mapping (org_unit,mapping_type,import_value,native_value) VALUES ('SYS1','profile','03','Elementary');
INSERT INTO patron_loader.mapping (org_unit,mapping_type,import_value,native_value) VALUES ('SYS1','profile','04','Elementary');
INSERT INTO patron_loader.mapping (org_unit,mapping_type,import_value,native_value) VALUES ('SYS1','profile','05','Middle 5th & 6th');
INSERT INTO patron_loader.mapping (org_unit,mapping_type,import_value,native_value) VALUES ('SYS1','profile','06','Middle 5th & 6th');
INSERT INTO patron_loader.mapping (org_unit,mapping_type,import_value,native_value) VALUES ('SYS1','profile','07','Middle 7th & 8th');
INSERT INTO patron_loader.mapping (org_unit,mapping_type,import_value,native_value/ VALUES ('SYS1','profile','08','Middle 7th & 8th');
INSERT INTO patron_loader.mapping (org_unit,mapping_type,import_value,native_value) VALUES ('SYS1','profile','09','High School');
INSERT INTO patron_loader.mapping (org_unit,mapping_type,import_value,native_value) VALUES ('SYS1','profile','10','High School');
INSERT INTO patron_loader.mapping (org_unit,mapping_type,import_value,native_value) VALUES ('SYS1','profile','11','High School');
INSERT INTO patron_loader.mapping (org_unit,mapping_type,import_value,native_value) VALUES ('SYS1','profile','12','High School');

INSERT INTO patron_loader.header (org_unit,import_header,default_header) VALUES ('SYS1','patron barcode','cardnumber');
*/
