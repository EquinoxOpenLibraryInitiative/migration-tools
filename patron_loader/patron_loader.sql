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



