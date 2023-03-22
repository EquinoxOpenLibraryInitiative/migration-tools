DROP TABLE IF EXISTS chars_to_normalize;

CREATE TABLE chars_to_normalize ( 
     id             SERIAL
    ,to_replace     TEXT
    ,replace_with   TEXT
);

INSERT INTO chars_to_normalize (to_replace,replace_with) VALUES 
-- spanish characters 
  ('á','a'), ('Á','A'), ('é','e'), ('É','E')
, ('í','i'), ('Í','I'), ('ó','o'), ('Ó','O')
, ('ú','u'), ('Ú','U'), ('ñ','n'), ('Ñ','N')
, ('ü','u'), ('Ü','U')
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

