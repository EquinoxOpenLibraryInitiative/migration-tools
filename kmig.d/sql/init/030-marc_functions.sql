
-- pass it the biblionumber, the position of the character to start in the leader starting at 0 
-- and the text to replace, single characer or multiple 
DROP FUNCTION IF EXISTS m_update_leader;
DELIMITER $
CREATE FUNCTION
   m_update_leader(bnumber INTEGER, ldr_position SMALLINT, new_value TEXT)
   RETURNS BOOLEAN
   DETERMINISTIC
    BEGIN
        DECLARE ldr TEXT COLLATE utf8mb4_unicode_ci DEFAULT NULL;
        DECLARE new_value_length SMALLINT DEFAULT 1;
        SET ldr_position = ldr_position + 1;
        SET new_value_length = LENGTH(new_value);
        
        SELECT ExtractValue(metadata, '//leader') INTO ldr FROM biblio_metadata WHERE biblionumber = bnumber;
        SET ldr = INSERT(ldr,ldr_position,new_value_length,new_value);
        SET ldr = CONCAT('<leader>',ldr,'</leader>');

        UPDATE biblio_metadata SET metadata = UpdateXML(metadata, '//leader', ldr) where biblionumber = bnumber;
        RETURN TRUE;
        
    END
$
DELIMITER ;

-- pass it the biblionumber and the 003 value (MARC org code) and it'll set the 003
-- to the passed value for the referenced bib
DROP FUNCTION IF EXISTS m_update_003;
DELIMITER $
CREATE FUNCTION
   m_update_003(bnumber INTEGER, marc_org_code TEXT COLLATE utf8mb4_unicode_ci)
   RETURNS BOOLEAN
   DETERMINISTIC
    BEGIN
        DECLARE ldr TEXT COLLATE utf8mb4_unicode_ci DEFAULT NULL;
        DECLARE tag003_count INT DEFAULT NULL;
        DECLARE marcxml TEXT COLLATE utf8mb4_unicode_ci DEFAULT NULL;

        SELECT metadata INTO marcxml FROM biblio_metadata WHERE biblionumber = bnumber;
        SELECT ExtractValue(metadata,'//leader') INTO ldr FROM biblio_metadata WHERE biblionumber = bnumber;
        SELECT ExtractValue(metadata,'count(//controlfield[@tag="003"])') INTO tag003_count FROM biblio_metadata WHERE biblionumber = bnumber;

        IF NULLIF(ldr,'') IS NULL THEN -- we need a leader
            RETURN FALSE;
        END IF;

        IF tag003_count > 1 THEN -- we need one or zero of these; it shouldn't be repeatable
            RETURN FALSE;
        END IF;

        -- handle 003 (insert or edit)
        IF tag003_count = 0 THEN
            SET marcxml = UpdateXML(
                marcxml,
                '//leader',
                CONCAT(
                    '<leader>',ldr,'</leader>\n',
                    '  <controlfield tag="003">',marc_org_code,'</controlfield>'
                )
            );
        ELSE
            SET marcxml = UpdateXML(
                marcxml,
                '//controlfield[@tag="003"]',
                CONCAT(
                    '<controlfield tag="003">',marc_org_code,'</controlfield>'
                )
            );
        END IF;

        UPDATE biblio_metadata SET metadata = marcxml where biblionumber = bnumber;
        RETURN TRUE;

    END
$
DELIMITER ;

-- Pass it the biblionumber, tag number, subfield, value and it'll add a MARC field to the end of the record accordingly
-- Be sure to escape the value if needed
-- Example: SELECT m_insert_tag(1,'909','a','foo');
DROP FUNCTION IF EXISTS m_insert_tag;
DELIMITER $
CREATE FUNCTION
   m_insert_tag(bnumber INTEGER, tag TEXT COLLATE utf8mb4_unicode_ci, subfield TEXT COLLATE utf8mb4_unicode_ci, value TEXT COLLATE utf8mb4_unicode_ci)
   RETURNS BOOLEAN
   DETERMINISTIC
    BEGIN
        DECLARE marcxml TEXT COLLATE utf8mb4_unicode_ci DEFAULT NULL;

        SELECT metadata INTO marcxml FROM biblio_metadata WHERE biblionumber = bnumber;

        IF NULLIF(marcxml,'') IS NULL THEN -- whaaa?
            RETURN FALSE;
        END IF;

        SET marcxml = replace(marcxml,'</record>',
            CONCAT(
                 '  <datafield tag="'
                ,tag
                ,'">\n    <subfield code="'
                ,subfield
                ,'">'
                ,value
                ,'</subfield>\n  </datafield>\n'
                ,'</record>'
            )
        );

        UPDATE biblio_metadata SET metadata = marcxml where biblionumber = bnumber;
        RETURN TRUE;

    END
$
DELIMITER ;
