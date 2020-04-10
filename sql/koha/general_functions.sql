DROP FUNCTION IF EXISTS split_string;
DELIMITER $
CREATE FUNCTION 
   split_string (s TEXT, del VARCHAR(10), i INT)
   RETURNS TEXT
   DETERMINISTIC
    BEGIN
        DECLARE n INT ;
        SET n = LENGTH(s) - LENGTH(REPLACE(s, del, '')) + 1;
        IF i > n THEN
            RETURN NULL ;
        ELSE
            RETURN SUBSTRING_INDEX(SUBSTRING_INDEX(s, del, i) , del , -1 ) ;        
        END IF;
    END
$
DELIMITER ;

DROP FUNCTION IF EXISTS string_segment_count;
DELIMITER $
CREATE FUNCTION 
   string_segment_count(s TEXT, del VARCHAR(10))
   RETURNS TEXT
   DETERMINISTIC
    BEGIN
        DECLARE n INT ;
        SET n = LENGTH(s) - LENGTH(REPLACE(s, del, '')) + 1;
        RETURN n;
    END
$
DELIMITER ;

DROP FUNCTION IF EXISTS remove_bracketed_text;
DELIMITER $
CREATE FUNCTION 
   remove_bracketed_text(str TEXT)
   RETURNS TEXT
   DETERMINISTIC
    BEGIN
    RETURN REPLACE(str, SUBSTRING(str, LOCATE('(', str), LENGTH(str) - LOCATE(')', REVERSE(str)) - LOCATE('(', str) + 2), '');
    END
$
DELIMITER ;

