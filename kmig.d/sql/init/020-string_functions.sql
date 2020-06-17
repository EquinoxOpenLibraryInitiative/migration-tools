DELIMITER $
CREATE FUNCTION
   m_remove_bracketed_text(str TEXT)
   RETURNS TEXT
   DETERMINISTIC
    BEGIN
    RETURN REPLACE(str, SUBSTRING(str, LOCATE('(', str), LENGTH(str) - LOCATE(')', REVERSE(str)) - LOCATE('(', str) + 2), '');
    END
$
DELIMITER ;

DROP FUNCTION IF EXISTS m_split_string;
DELIMITER $
CREATE FUNCTION 
   m_split_string (s TEXT, del VARCHAR(10), i INT)
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

DROP FUNCTION IF EXISTS m_string_segment_count;
DELIMITER $
CREATE FUNCTION 
   m_string_segment_count(s TEXT, del VARCHAR(10))
   RETURNS TEXT
   DETERMINISTIC
    BEGIN
        DECLARE n INT ;
        SET n = LENGTH(s) - LENGTH(REPLACE(s, del, '')) + 1;
        RETURN n;
    END
$
DELIMITER ;

DROP FUNCTION IF EXISTS m_remove_nonalpha;
delimiter $
CREATE FUNCTION m_remove_nonalpha( s CHAR(255) ) RETURNS CHAR(255) DETERMINISTIC
    BEGIN
      DECLARE var1, length SMALLINT DEFAULT 1;
      DECLARE result CHAR(255) DEFAULT '';
      DECLARE ch CHAR(1);
      SET length  = CHAR_LENGTH( s );
      REPEAT
        BEGIN
          SET ch = MID( s, var1, 1 );
          IF ch REGEXP '[[:alnum:]]' THEN
            SET result =CONCAT(result ,ch);
          END IF;
          SET var1 = var1 + 1;
        END;
      UNTIL var1 >length  END REPEAT;
      RETURN result ;
    END 
  $
  DELIMITER ;
