
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


