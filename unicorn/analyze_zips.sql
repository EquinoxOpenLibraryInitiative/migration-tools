-- Obviously you'll want to change 'm_pioneer', and if you want to port this to other ILSes,
-- you'll want to change l_user_addr1_zip, l_user_city_state, l_user_addr1_county, etc.


DROP TABLE IF EXISTS m_pioneer.zips;

CREATE TABLE m_pioneer.zips (
  zipcode TEXT,
  city_state TEXT,
  county TEXT,
  num INT
);

CREATE OR REPLACE FUNCTION analyze_zips() RETURNS VOID AS $$

DECLARE
  zip TEXT;
  
BEGIN

FOR zip IN 
  SELECT DISTINCT SUBSTRING(l_user_addr1_zip FROM 1 FOR 5)
  FROM m_pioneer.actor_usr_legacy
  WHERE l_user_addr1_zip <> ''
  LOOP
    INSERT INTO m_pioneer.zips (zipcode, city_state, county, num) 
      SELECT zip, l_user_addr1_city_state, l_user_addr1_county, count(*)
        FROM m_pioneer.actor_usr_legacy 
        WHERE l_user_addr1_zip=zip 
        GROUP BY 1,2,3;
  END LOOP;

END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION report_zips() RETURNS SETOF m_pioneer.zips AS $$

DECLARE
  zip TEXT;
  output m_pioneer.zips%ROWTYPE;  

BEGIN

FOR zip IN 
  SELECT DISTINCT zipcode
    FROM m_pioneer.zips
  LOOP
SELECT INTO OUTPUT *
  FROM m_pioneer.zips
  WHERE num = (	SELECT MAX(num) FROM m_pioneer.zips WHERE zipcode=zip ) AND zipcode=zip
  LIMIT 1;
  RETURN NEXT output;
END LOOP;

END;

$$ LANGUAGE plpgsql;

SELECT analyze_zips();

SELECT * FROM report_zips() ORDER BY num DESC;

