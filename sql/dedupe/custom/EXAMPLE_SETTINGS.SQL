\timing on

DROP TABLE IF EXISTS dedupe_features;
CREATE TABLE dedupe_features (id SERIAL, name TEXT, org TEXT, value TEXT, value2 TEXT);

INSERT INTO dedupe_features (name, org, value) VALUES ('use_staging_bibs', 'EXAMPLELIB', 'inclusive');
INSERT INTO dedupe_features (name, org, value) VALUES ('incoming floor', 'EXAMPLELIB', '1');
INSERT INTO dedupe_features (name, org, value) VALUES ('incoming ceiling', 'EXAMPLELIB', '9223372036854775807');

DO $$
DECLARE
    loaded_count BIGINT DEFAULT 0;
BEGIN
    IF EXISTS(SELECT 1 FROM m_biblio_record_entry WHERE id > 0 LIMIT 1) THEN

        UPDATE dedupe_features SET value = (SELECT MIN(id) FROM m_biblio_record_entry)::TEXT WHERE name = 'incoming floor';
        UPDATE dedupe_features SET value = (SELECT MAX(id) FROM m_biblio_record_entry)::TEXT WHERE name = 'incoming ceiling';
    
        SELECT COUNT(*) FROM biblio.record_entry WHERE id IN (SELECT id FROM m_biblio_record_entry) INTO loaded_count;
    
        IF loaded_count > 0 THEN
            UPDATE dedupe_features SET value = 'subset' WHERE name = 'dedupe_type';
        ELSE
            UPDATE dedupe_features SET value = 'migration' WHERE name = 'dedupe_type';
        END IF;
    END IF;
END $$;

INSERT INTO dedupe_features (name, org, value) VALUES ('incoming floor', 'EXAMPLELIB', '1');
INSERT INTO dedupe_features (name, org, value) VALUES ('keep secondary oclcs', 'EXAMPLELIB', 'FALSE');
INSERT INTO dedupe_features (name, org, value) VALUES ('skip acq records', 'EXAMPLELIB', 'TRUE');
INSERT INTO dedupe_features (name, org, value) VALUES ('skip parts', 'EXAMPLELIB', 'FALSE');
INSERT INTO dedupe_features (name, org, value) VALUES ('derived sf on book', 'EXAMPLELIB', 'FALSE');
INSERT INTO dedupe_features (name, org, value) VALUES ('derived sf on cdaudiobook', 'EXAMPLELIB', 'FALSE');
INSERT INTO dedupe_features (name, org, value) VALUES ('derived sf on dvd', 'EXAMPLELIB', 'FALSE');
INSERT INTO dedupe_features (name, org, value) VALUES ('derived sf on blu-ray', 'EXAMPLELIB', 'FALSE');
INSERT INTO dedupe_features (name, org, value) VALUES ('merge tacs isbn', 'EXAMPLELIB', 'TRUE');
INSERT INTO dedupe_features (name, org, value) VALUES ('merge tacs issn', 'EXAMPLELIB', 'TRUE');
INSERT INTO dedupe_features (name, org, value) VALUES ('merge tacs upc', 'EXAMPLELIB', 'TRUE');
INSERT INTO dedupe_features (name, org, value) VALUES ('merge tacs pubdate', 'EXAMPLELIB', 'TRUE');
INSERT INTO dedupe_features (name, org, value) VALUES ('merge authorless av', 'EXAMPLELIB', 'TRUE');
INSERT INTO dedupe_features (name, org, value) VALUES ('merge tac manga', 'EXAMPLELIB', 'TRUE');
INSERT INTO dedupe_features (name, org, value) VALUES ('merge local history', 'EXAMPLELIB', 'FALSE');
INSERT INTO dedupe_features (name, org, value) VALUES ('remove aged holds', 'EXAMPLELIB', '200 YEARS');
INSERT INTO dedupe_features (name, org, value) VALUES ('remove childless bibs', 'EXAMPLELIB', '180 days');
INSERT INTO dedupe_features (name, org, value) VALUES ('remove childless volumes', 'EXAMPLELIB', '180 days');
INSERT INTO dedupe_features (name, org, value) VALUES ('6XX scoring', 'EXAMPLELIB', 'lc primary');
INSERT INTO dedupe_features (name, org, value) VALUES ('manga publisher','EXAMPLELIB','kodansha');
INSERT INTO dedupe_features (name, org, value) VALUES ('manga publisher','EXAMPLELIB','viz');
INSERT INTO dedupe_features (name, org, value) VALUES ('manga publisher','EXAMPLELIB','dccomics');
INSERT INTO dedupe_features (name, org, value) VALUES ('manga publisher','EXAMPLELIB','marvel');
INSERT INTO dedupe_features (name, org, value) VALUES ('manga publisher','EXAMPLELIB','darkhorse');
INSERT INTO dedupe_features (name, org, value) VALUES ('manga publisher','EXAMPLELIB','imagecomics');
INSERT INTO dedupe_features (name, org, value) VALUES ('manga publisher','EXAMPLELIB','topcow');
