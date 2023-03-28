DROP TABLE IF EXISTS chars_to_normalize;

CREATE TABLE chars_to_normalize ( 
     id             SERIAL
    ,to_replace     TEXT
    ,replace_with   TEXT
);

INSERT INTO chars_to_normalize (to_replace,replace_with) VALUES 
-- judgement call since there is no telling what catalogers will do 
-- common unicode characters 
  ('á','a'), ('Á','A'), ('à','a'), ('À','A')
, ('â','a'), ('Â','A'), ('ä','a'), ('Ä','A')  
, ('é','e'), ('É','E'), ('è','e'), ('È','E')
, ('ê','e'), ('Ê','E'), ('ë','e'), ('Ë','E')
, ('í','i'), ('Í','I')
, ('î','i'), ('Î','I'), ('ï','i'), ('Ï','I')
, ('ó','o'), ('Ó','O'), ('ô','o'), ('Ô','O')
, ('ö','o'), ('Ö','O')
, ('ú','u'), ('Ú','U'), ('ü','u'), ('Ü','U')
, ('ù','u'), ('Ù','U'), ('û','u'), ('Û','U')
, ('ÿ','y'), ('Ÿ','Y')
, ('ñ','n'), ('Ñ','N')
-- , ('ç','c'), ('Ç','C') on the fence

-- numbers to words 
, ('0','zero')
, ('1','one')
, ('2','two')
, ('3','three')
, ('4','four')
, ('5','five')
, ('6','six')
, ('7','seven')
, ('8','eight')
, ('9','nine')
-- misc 
, ('&','and')
;

DROP TABLE IF EXISTS title_strings;

CREATE TABLE title_strings (
     id             SERIAL
    ,to_replace     TEXT
    ,replace_with   TEXT
);

INSERT INTO title_strings (to_replace,replace_with) VALUES
    (' videorecording ',' '),
    (' videorecording$',''),
    ('^videorecording ',''),
    (' dvd ',' '),
    (' dvd$',''),
    ('^dvd ',''),
    (' vhs ',' '),
    (' vhs$',''),
    ('^vhs ',''),
    (' videorecording ',' '),
    (' videorecording$',''),
    ('^videorecording ',''),
    (' hc ',' '),
    (' hc$',''),
    ('^hc ',''),
    (' pb ',' '),
    (' pb$',''),
    ('^pb ',''),
    (' pbk ',' '),
    (' pbk$',''),
    ('^pbk ',''),
    (' a ',' '),
    (' a$',''),
    ('^a ',''),
    (' the ',' '),
    (' the$',''),
    ('^the ',''),
    (' novel ',' '),
    (' novel$',''),
    ('^novel ',''),
    ('\[(.*?)\]',''),
    ('\((.*?)\)','')
;

