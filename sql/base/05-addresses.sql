CREATE OR REPLACE FUNCTION migration_tools.attempt_phone (TEXT,TEXT) RETURNS TEXT AS $$
  DECLARE
    phone TEXT := $1;
    areacode TEXT := $2;
    temp TEXT := '';
    output TEXT := '';
    n_digits INTEGER := 0;
  BEGIN
    temp := phone;
    temp := REGEXP_REPLACE(temp, '^1*[^0-9]*(?=[0-9])', '');
    temp := REGEXP_REPLACE(temp, '[^0-9]*([0-9]{3})[^0-9]*([0-9]{3})[^0-9]*([0-9]{4})', E'\\1-\\2-\\3');
    n_digits := LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(temp, '(.*)?[a-zA-Z].*', E'\\1') , '[^0-9]', '', 'g'));
    IF n_digits = 7 AND areacode <> '' THEN
      temp := REGEXP_REPLACE(temp, '[^0-9]*([0-9]{3})[^0-9]*([0-9]{4})', E'\\1-\\2');
      output := (areacode || '-' || temp);
    ELSE
      output := temp;
    END IF;
    RETURN output;
  END;

$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.address_parse_out_citystatezip (TEXT) RETURNS TEXT[] AS $$
    DECLARE
        city_state_zip TEXT := $1;
        city TEXT := '';
        state TEXT := '';
        zip TEXT := '';
    BEGIN
        zip := CASE WHEN city_state_zip ~ E'\\d\\d\\d\\d\\d' THEN REGEXP_REPLACE( city_state_zip, E'^.*(\\d\\d\\d\\d\\d-?\\d*).*$', E'\\1' ) ELSE '' END;
        city_state_zip := REGEXP_REPLACE( city_state_zip, E'^(.*)\\d\\d\\d\\d\\d-?\\d*(.*)$', E'\\1\\2');
        IF city_state_zip ~ ',' THEN
            state := REGEXP_REPLACE( city_state_zip, E'^(.*),(.*)$', E'\\2');
            city := REGEXP_REPLACE( city_state_zip, E'^(.*),(.*)$', E'\\1');
        ELSE
            IF city_state_zip ~ E'\\s+[A-Z][A-Z]\\s*$' THEN
                state := REGEXP_REPLACE( city_state_zip, E'^.*,?\\s+([A-Z][A-Z])\\s*$', E'\\1' );
                city := REGEXP_REPLACE( city_state_zip, E'^(.*?),?\\s+[A-Z][A-Z](\\s*)$', E'\\1\\2' );
            ELSE
                IF city_state_zip ~ E'^\\S+$'  THEN
                    city := city_state_zip;
                    state := 'N/A';
                ELSE
                    state := REGEXP_REPLACE( city_state_zip, E'^(.*?),?\\s*(\\S+)\\s*$', E'\\2');
                    city := REGEXP_REPLACE( city_state_zip, E'^(.*?),?\\s*(\\S+)\\s*$', E'\\1');
                END IF;
            END IF;
        END IF;
        RETURN ARRAY[ TRIM(BOTH ' ' FROM city), TRIM(BOTH ' ' FROM state), TRIM(BOTH ' ' FROM zip) ];
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- try to parse data like this: 100 MAIN ST$COVINGTON, GA 30016
CREATE OR REPLACE FUNCTION migration_tools.parse_out_address (TEXT) RETURNS TEXT[] AS $$
    DECLARE
        fullstring TEXT := $1;
        address1 TEXT := '';
        address2 TEXT := '';
        scratch1 TEXT := '';
        scratch2 TEXT := '';
        city TEXT := '';
        state TEXT := '';
        zip TEXT := '';
    BEGIN
        zip := CASE
            WHEN fullstring ~ E'\\d\\d\\d\\d\\d'
            THEN REGEXP_REPLACE( fullstring, E'^.*(\\d\\d\\d\\d\\d-?\\d*).*$', E'\\1' )
            ELSE ''
        END;
        fullstring := REGEXP_REPLACE( fullstring, E'^(.*)\\d\\d\\d\\d\\d-?\\d*(.*)$', E'\\1\\2');

        IF fullstring ~ ',' THEN
            state := REGEXP_REPLACE( fullstring, E'^(.*),(.*)$', E'\\2');
            scratch1 := REGEXP_REPLACE( fullstring, E'^(.*),(.*)$', E'\\1');
        ELSE
            IF fullstring ~ E'\\s+[A-Z][A-Z]\\s*$' THEN
                state := REGEXP_REPLACE( fullstring, E'^.*,?\\s+([A-Z][A-Z])\\s*$', E'\\1' );
                scratch1 := REGEXP_REPLACE( fullstring, E'^(.*?),?\\s+[A-Z][A-Z](\\s*)$', E'\\1\\2' );
            ELSE
                IF fullstring ~ E'^\\S+$'  THEN
                    scratch1 := fullstring;
                    state := 'N/A';
                ELSE
                    state := REGEXP_REPLACE( fullstring, E'^(.*?),?\\s*(\\S+)\\s*$', E'\\2');
                    scratch1 := REGEXP_REPLACE( fullstring, E'^(.*?),?\\s*(\\S+)\\s*$', E'\\1');
                END IF;
            END IF;
        END IF;

        IF scratch1 ~ '[\$]' THEN
            scratch2 := REGEXP_REPLACE( scratch1, E'^(.+)[\$](.+?)$', E'\\1');
            city := REGEXP_REPLACE( scratch1, E'^(.+)[\$](.+?)$', E'\\2');
        ELSE
            IF scratch1 ~ '\s' THEN
                scratch2 := REGEXP_REPLACE( scratch1, E'^(.+)\\s+(.+?)$', E'\\1');
                city := REGEXP_REPLACE( scratch1, E'^(.+)\\s+(.+?)$', E'\\2');
            ELSE
                scratch2 := 'N/A';
                city := scratch1;
            END IF;
        END IF;

        IF scratch2 ~ '^\d' THEN
            address1 := scratch2;
            address2 := '';
        ELSE
            address1 := REGEXP_REPLACE( scratch2, E'^(.+?)(\\d.+)$', E'\\1');
            address2 := REGEXP_REPLACE( scratch2, E'^(.+?)(\\d.+)$', E'\\2');
        END IF;

        RETURN ARRAY[
             TRIM(BOTH ' ' FROM address1)
            ,TRIM(BOTH ' ' FROM address2)
            ,TRIM(BOTH ' ' FROM city)
            ,TRIM(BOTH ' ' FROM state)
            ,TRIM(BOTH ' ' FROM zip)
        ];
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.parse_out_address2 (TEXT) RETURNS TEXT AS $$
    my ($address) = @_;

    use Geo::StreetAddress::US;

    my $a = Geo::StreetAddress::US->parse_location($address);

    return [
         "$a->{number} $a->{prefix} $a->{street} $a->{type} $a->{suffix}"
        ,"$a->{sec_unit_type} $a->{sec_unit_num}"
        ,$a->{city}
        ,$a->{state}
        ,$a->{zip}
    ];
$$ LANGUAGE PLPERLU STABLE;

DROP TABLE IF EXISTS migration_tools.usps_suffixes;
CREATE TABLE migration_tools.usps_suffixes ( suffix_from TEXT, suffix_to TEXT );
INSERT INTO migration_tools.usps_suffixes VALUES
    ('ALLEE','ALY'),
    ('ALLEY','ALY'),
    ('ALLY','ALY'),
    ('ALY','ALY'),
    ('ANEX','ANX'),
    ('ANNEX','ANX'),
    ('ANNX','ANX'),
    ('ANX','ANX'),
    ('ARCADE','ARC'),
    ('ARC','ARC'),
    ('AV','AVE'),
    ('AVE','AVE'),
    ('AVEN','AVE'),
    ('AVENU','AVE'),
    ('AVENUE','AVE'),
    ('AVN','AVE'),
    ('AVNUE','AVE'),
    ('BAYOO','BYU'),
    ('BAYOU','BYU'),
    ('BCH','BCH'),
    ('BEACH','BCH'),
    ('BEND','BND'),
    ('BLF','BLF'),
    ('BLUF','BLF'),
    ('BLUFF','BLF'),
    ('BLUFFS','BLFS'),
    ('BLVD','BLVD'),
    ('BND','BND'),
    ('BOT','BTM'),
    ('BOTTM','BTM'),
    ('BOTTOM','BTM'),
    ('BOUL','BLVD'),
    ('BOULEVARD','BLVD'),
    ('BOULV','BLVD'),
    ('BRANCH','BR'),
    ('BR','BR'),
    ('BRDGE','BRG'),
    ('BRG','BRG'),
    ('BRIDGE','BRG'),
    ('BRK','BRK'),
    ('BRNCH','BR'),
    ('BROOK','BRK'),
    ('BROOKS','BRKS'),
    ('BTM','BTM'),
    ('BURG','BG'),
    ('BURGS','BGS'),
    ('BYPA','BYP'),
    ('BYPAS','BYP'),
    ('BYPASS','BYP'),
    ('BYP','BYP'),
    ('BYPS','BYP'),
    ('CAMP','CP'),
    ('CANYN','CYN'),
    ('CANYON','CYN'),
    ('CAPE','CPE'),
    ('CAUSEWAY','CSWY'),
    ('CAUSWAY','CSWY'),
    ('CEN','CTR'),
    ('CENT','CTR'),
    ('CENTER','CTR'),
    ('CENTERS','CTRS'),
    ('CENTR','CTR'),
    ('CENTRE','CTR'),
    ('CIRC','CIR'),
    ('CIR','CIR'),
    ('CIRCL','CIR'),
    ('CIRCLE','CIR'),
    ('CIRCLES','CIRS'),
    ('CK','CRK'),
    ('CLB','CLB'),
    ('CLF','CLF'),
    ('CLFS','CLFS'),
    ('CLIFF','CLF'),
    ('CLIFFS','CLFS'),
    ('CLUB','CLB'),
    ('CMP','CP'),
    ('CNTER','CTR'),
    ('CNTR','CTR'),
    ('CNYN','CYN'),
    ('COMMON','CMN'),
    ('COR','COR'),
    ('CORNER','COR'),
    ('CORNERS','CORS'),
    ('CORS','CORS'),
    ('COURSE','CRSE'),
    ('COURT','CT'),
    ('COURTS','CTS'),
    ('COVE','CV'),
    ('COVES','CVS'),
    ('CP','CP'),
    ('CPE','CPE'),
    ('CRCL','CIR'),
    ('CRCLE','CIR'),
    ('CR','CRK'),
    ('CRECENT','CRES'),
    ('CREEK','CRK'),
    ('CRESCENT','CRES'),
    ('CRES','CRES'),
    ('CRESENT','CRES'),
    ('CREST','CRST'),
    ('CRK','CRK'),
    ('CROSSING','XING'),
    ('CROSSROAD','XRD'),
    ('CRSCNT','CRES'),
    ('CRSE','CRSE'),
    ('CRSENT','CRES'),
    ('CRSNT','CRES'),
    ('CRSSING','XING'),
    ('CRSSNG','XING'),
    ('CRT','CT'),
    ('CSWY','CSWY'),
    ('CT','CT'),
    ('CTR','CTR'),
    ('CTS','CTS'),
    ('CURVE','CURV'),
    ('CV','CV'),
    ('CYN','CYN'),
    ('DALE','DL'),
    ('DAM','DM'),
    ('DIV','DV'),
    ('DIVIDE','DV'),
    ('DL','DL'),
    ('DM','DM'),
    ('DR','DR'),
    ('DRIV','DR'),
    ('DRIVE','DR'),
    ('DRIVES','DRS'),
    ('DRV','DR'),
    ('DVD','DV'),
    ('DV','DV'),
    ('ESTATE','EST'),
    ('ESTATES','ESTS'),
    ('EST','EST'),
    ('ESTS','ESTS'),
    ('EXP','EXPY'),
    ('EXPRESS','EXPY'),
    ('EXPRESSWAY','EXPY'),
    ('EXPR','EXPY'),
    ('EXPW','EXPY'),
    ('EXPY','EXPY'),
    ('EXTENSION','EXT'),
    ('EXTENSIONS','EXTS'),
    ('EXT','EXT'),
    ('EXTN','EXT'),
    ('EXTNSN','EXT'),
    ('EXTS','EXTS'),
    ('FALL','FALL'),
    ('FALLS','FLS'),
    ('FERRY','FRY'),
    ('FIELD','FLD'),
    ('FIELDS','FLDS'),
    ('FLAT','FLT'),
    ('FLATS','FLTS'),
    ('FLD','FLD'),
    ('FLDS','FLDS'),
    ('FLS','FLS'),
    ('FLT','FLT'),
    ('FLTS','FLTS'),
    ('FORD','FRD'),
    ('FORDS','FRDS'),
    ('FOREST','FRST'),
    ('FORESTS','FRST'),
    ('FORGE','FRG'),
    ('FORGES','FRGS'),
    ('FORG','FRG'),
    ('FORK','FRK'),
    ('FORKS','FRKS'),
    ('FORT','FT'),
    ('FRD','FRD'),
    ('FREEWAY','FWY'),
    ('FREEWY','FWY'),
    ('FRG','FRG'),
    ('FRK','FRK'),
    ('FRKS','FRKS'),
    ('FRRY','FRY'),
    ('FRST','FRST'),
    ('FRT','FT'),
    ('FRWAY','FWY'),
    ('FRWY','FWY'),
    ('FRY','FRY'),
    ('FT','FT'),
    ('FWY','FWY'),
    ('GARDEN','GDN'),
    ('GARDENS','GDNS'),
    ('GARDN','GDN'),
    ('GATEWAY','GTWY'),
    ('GATEWY','GTWY'),
    ('GATWAY','GTWY'),
    ('GDN','GDN'),
    ('GDNS','GDNS'),
    ('GLEN','GLN'),
    ('GLENS','GLNS'),
    ('GLN','GLN'),
    ('GRDEN','GDN'),
    ('GRDN','GDN'),
    ('GRDNS','GDNS'),
    ('GREEN','GRN'),
    ('GREENS','GRNS'),
    ('GRN','GRN'),
    ('GROVE','GRV'),
    ('GROVES','GRVS'),
    ('GROV','GRV'),
    ('GRV','GRV'),
    ('GTWAY','GTWY'),
    ('GTWY','GTWY'),
    ('HARB','HBR'),
    ('HARBOR','HBR'),
    ('HARBORS','HBRS'),
    ('HARBR','HBR'),
    ('HAVEN','HVN'),
    ('HAVN','HVN'),
    ('HBR','HBR'),
    ('HEIGHT','HTS'),
    ('HEIGHTS','HTS'),
    ('HGTS','HTS'),
    ('HIGHWAY','HWY'),
    ('HIGHWY','HWY'),
    ('HILL','HL'),
    ('HILLS','HLS'),
    ('HIWAY','HWY'),
    ('HIWY','HWY'),
    ('HL','HL'),
    ('HLLW','HOLW'),
    ('HLS','HLS'),
    ('HOLLOW','HOLW'),
    ('HOLLOWS','HOLW'),
    ('HOLW','HOLW'),
    ('HOLWS','HOLW'),
    ('HRBOR','HBR'),
    ('HT','HTS'),
    ('HTS','HTS'),
    ('HVN','HVN'),
    ('HWAY','HWY'),
    ('HWY','HWY'),
    ('INLET','INLT'),
    ('INLT','INLT'),
    ('IS','IS'),
    ('ISLAND','IS'),
    ('ISLANDS','ISS'),
    ('ISLANDS','SLNDS'),
    ('ISLANDS','SS'),
    ('ISLE','ISLE'),
    ('ISLES','ISLE'),
    ('ISLND','IS'),
    ('I','SLNDS'),
    ('ISS','ISS'),
    ('JCTION','JCT'),
    ('JCT','JCT'),
    ('JCTN','JCT'),
    ('JCTNS','JCTS'),
    ('JCTS','JCTS'),
    ('JUNCTION','JCT'),
    ('JUNCTIONS','JCTS'),
    ('JUNCTN','JCT'),
    ('JUNCTON','JCT'),
    ('KEY','KY'),
    ('KEYS','KYS'),
    ('KNL','KNL'),
    ('KNLS','KNLS'),
    ('KNOL','KNL'),
    ('KNOLL','KNL'),
    ('KNOLLS','KNLS'),
    ('KY','KY'),
    ('KYS','KYS'),
    ('LAKE','LK'),
    ('LAKES','LKS'),
    ('LA','LN'),
    ('LANDING','LNDG'),
    ('LAND','LAND'),
    ('LANE','LN'),
    ('LANES','LN'),
    ('LCK','LCK'),
    ('LCKS','LCKS'),
    ('LDGE','LDG'),
    ('LDG','LDG'),
    ('LF','LF'),
    ('LGT','LGT'),
    ('LIGHT','LGT'),
    ('LIGHTS','LGTS'),
    ('LK','LK'),
    ('LKS','LKS'),
    ('LNDG','LNDG'),
    ('LNDNG','LNDG'),
    ('LN','LN'),
    ('LOAF','LF'),
    ('LOCK','LCK'),
    ('LOCKS','LCKS'),
    ('LODGE','LDG'),
    ('LODG','LDG'),
    ('LOOP','LOOP'),
    ('LOOPS','LOOP'),
    ('MALL','MALL'),
    ('MANOR','MNR'),
    ('MANORS','MNRS'),
    ('MDW','MDW'),
    ('MDWS','MDWS'),
    ('MEADOW','MDW'),
    ('MEADOWS','MDWS'),
    ('MEDOWS','MDWS'),
    ('MEWS','MEWS'),
    ('MILL','ML'),
    ('MILLS','MLS'),
    ('MISSION','MSN'),
    ('MISSN','MSN'),
    ('ML','ML'),
    ('MLS','MLS'),
    ('MNR','MNR'),
    ('MNRS','MNRS'),
    ('MNTAIN','MTN'),
    ('MNT','MT'),
    ('MNTN','MTN'),
    ('MNTNS','MTNS'),
    ('MOTORWAY','MTWY'),
    ('MOUNTAIN','MTN'),
    ('MOUNTAINS','MTNS'),
    ('MOUNTIN','MTN'),
    ('MOUNT','MT'),
    ('MSN','MSN'),
    ('MSSN','MSN'),
    ('MTIN','MTN'),
    ('MT','MT'),
    ('MTN','MTN'),
    ('NCK','NCK'),
    ('NECK','NCK'),
    ('ORCHARD','ORCH'),
    ('ORCH','ORCH'),
    ('ORCHRD','ORCH'),
    ('OVAL','OVAL'),
    ('OVERPASS','OPAS'),
    ('OVL','OVAL'),
    ('PARK','PARK'),
    ('PARKS','PARK'),
    ('PARKWAY','PKWY'),
    ('PARKWAYS','PKWY'),
    ('PARKWY','PKWY'),
    ('PASSAGE','PSGE'),
    ('PASS','PASS'),
    ('PATH','PATH'),
    ('PATHS','PATH'),
    ('PIKE','PIKE'),
    ('PIKES','PIKE'),
    ('PINE','PNE'),
    ('PINES','PNES'),
    ('PK','PARK'),
    ('PKWAY','PKWY'),
    ('PKWY','PKWY'),
    ('PKWYS','PKWY'),
    ('PKY','PKWY'),
    ('PLACE','PL'),
    ('PLAINES','PLNS'),
    ('PLAIN','PLN'),
    ('PLAINS','PLNS'),
    ('PLAZA','PLZ'),
    ('PLN','PLN'),
    ('PLNS','PLNS'),
    ('PL','PL'),
    ('PLZA','PLZ'),
    ('PLZ','PLZ'),
    ('PNES','PNES'),
    ('POINT','PT'),
    ('POINTS','PTS'),
    ('PORT','PRT'),
    ('PORTS','PRTS'),
    ('PRAIRIE','PR'),
    ('PRARIE','PR'),
    ('PRK','PARK'),
    ('PR','PR'),
    ('PRR','PR'),
    ('PRT','PRT'),
    ('PRTS','PRTS'),
    ('PT','PT'),
    ('PTS','PTS'),
    ('RADIAL','RADL'),
    ('RADIEL','RADL'),
    ('RADL','RADL'),
    ('RAD','RADL'),
    ('RAMP','RAMP'),
    ('RANCHES','RNCH'),
    ('RANCH','RNCH'),
    ('RAPID','RPD'),
    ('RAPIDS','RPDS'),
    ('RDGE','RDG'),
    ('RDG','RDG'),
    ('RDGS','RDGS'),
    ('RD','RD'),
    ('RDS','RDS'),
    ('REST','RST'),
    ('RIDGE','RDG'),
    ('RIDGES','RDGS'),
    ('RIVER','RIV'),
    ('RIV','RIV'),
    ('RIVR','RIV'),
    ('RNCH','RNCH'),
    ('RNCHS','RNCH'),
    ('ROAD','RD'),
    ('ROADS','RDS'),
    ('ROUTE','RTE'),
    ('ROW','ROW'),
    ('RPD','RPD'),
    ('RPDS','RPDS'),
    ('RST','RST'),
    ('RUE','RUE'),
    ('RUN','RUN'),
    ('RVR','RIV'),
    ('SHL','SHL'),
    ('SHLS','SHLS'),
    ('SHOAL','SHL'),
    ('SHOALS','SHLS'),
    ('SHOAR','SHR'),
    ('SHOARS','SHRS'),
    ('SHORE','SHR'),
    ('SHORES','SHRS'),
    ('SHR','SHR'),
    ('SHRS','SHRS'),
    ('SKYWAY','SKWY'),
    ('SMT','SMT'),
    ('SPG','SPG'),
    ('SPGS','SPGS'),
    ('SPNG','SPG'),
    ('SPNGS','SPGS'),
    ('SPRING','SPG'),
    ('SPRINGS','SPGS'),
    ('SPRNG','SPG'),
    ('SPRNGS','SPGS'),
    ('SPUR','SPUR'),
    ('SPURS','SPUR'),
    ('SQRE','SQ'),
    ('SQR','SQ'),
    ('SQRS','SQS'),
    ('SQ','SQ'),
    ('SQUARE','SQ'),
    ('SQUARES','SQS'),
    ('SQU','SQ'),
    ('STA','STA'),
    ('STATION','STA'),
    ('STATN','STA'),
    ('STN','STA'),
    ('STRA','STRA'),
    ('STRAVEN','STRA'),
    ('STRAVENUE','STRA'),
    ('STRAVE','STRA'),
    ('STRAVN','STRA'),
    ('STRAV','STRA'),
    ('STREAM','STRM'),
    ('STREETS','STS'),
    ('STREET','ST'),
    ('STREME','STRM'),
    ('STRM','STRM'),
    ('STR','ST'),
    ('STRT','ST'),
    ('STRVN','STRA'),
    ('STRVNUE','STRA'),
    ('ST','ST'),
    ('SUMIT','SMT'),
    ('SUMITT','SMT'),
    ('SUMMIT','SMT'),
    ('TERRACE','TER'),
    ('TERR','TER'),
    ('TER','TER'),
    ('THROUGHWAY','TRWY'),
    ('TPKE','TPKE'),
    ('TPK','TPKE'),
    ('TRACES','TRCE'),
    ('TRACE','TRCE'),
    ('TRACKS','TRAK'),
    ('TRACK','TRAK'),
    ('TRAFFICWAY','TRFY'),
    ('TRAILS','TRL'),
    ('TRAIL','TRL'),
    ('TRAK','TRAK'),
    ('TRCE','TRCE'),
    ('TRFY','TRFY'),
    ('TRKS','TRAK'),
    ('TRK','TRAK'),
    ('TRLS','TRL'),
    ('TRL','TRL'),
    ('TRNPK','TPKE'),
    ('TRPK','TPKE'),
    ('TR','TRL'),
    ('TUNEL','TUNL'),
    ('TUNLS','TUNL'),
    ('TUNL','TUNL'),
    ('TUNNELS','TUNL'),
    ('TUNNEL','TUNL'),
    ('TUNNL','TUNL'),
    ('TURNPIKE','TPKE'),
    ('TURNPK','TPKE'),
    ('UNDERPASS','UPAS'),
    ('UNIONS','UNS'),
    ('UNION','UN'),
    ('UN','UN'),
    ('VALLEYS','VLYS'),
    ('VALLEY','VLY'),
    ('VALLY','VLY'),
    ('VDCT','IA'),
    ('VIADCT','VIA'),
    ('VIADUCT','IA'),
    ('VIADUCT','VIA'),
    ('VIA','VIA'),
    ('VIEWS','VWS'),
    ('VIEW','VW'),
    ('VILLAGES','VLGS'),
    ('VILLAGE','VLG'),
    ('VILLAG','VLG'),
    ('VILLE','VL'),
    ('VILLG','VLG'),
    ('VILLIAGE','VLG'),
    ('VILL','VLG'),
    ('VISTA','VIS'),
    ('VIST','VIS'),
    ('VIS','VIS'),
    ('VLGS','VLGS'),
    ('VLG','VLG'),
    ('VLLY','VLY'),
    ('VL','VL'),
    ('VLYS','VLYS'),
    ('VLY','VLY'),
    ('VSTA','VIS'),
    ('VST','VIS'),
    ('VWS','VWS'),
    ('VW','VW'),
    ('WALKS','WALK'),
    ('WALK','WALK'),
    ('WALL','WALL'),
    ('WAYS','WAYS'),
    ('WAY','WAY'),
    ('WELLS','WLS'),
    ('WELL','WL'),
    ('WLS','WLS'),
    ('WY','WAY'),
    ('XING','XING');

-- this function should get a smaller range of inputs and benefit more from STABLE, hopefully speeding things up
CREATE OR REPLACE FUNCTION migration_tools._normalize_address_suffix (TEXT) RETURNS TEXT AS $$
    DECLARE
        suffix TEXT := $1;
		_r RECORD;
    BEGIN
        --RAISE INFO 'suffix = %', suffix;
		FOR _r IN (SELECT * FROM migration_tools.usps_suffixes) LOOP
			suffix := REGEXP_REPLACE( suffix, _r.suffix_from, _r.suffix_to, 'i');
		END LOOP;
		RETURN suffix;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.normalize_address_suffix (TEXT) RETURNS TEXT AS $$
    BEGIN
		RETURN CASE
            WHEN $1 ~ '\s\S+$' THEN REGEXP_REPLACE( $1, '^(.*\s)(\S+)$', '\1' ) || migration_tools._normalize_address_suffix( REGEXP_REPLACE( $1, '^(.*\s)(\S+)$', '\2' ) )
            ELSE $1
        END;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.zip_to_city_state_county (TEXT) RETURNS TEXT[] AS $$

	my $input = $_[0];
	my %zipdata;

	open (FH, '<', '/openils/var/data/zips.txt') or return ('No File Found', 'No File Found', 'No File Found');

	while (<FH>) {
		chomp;
		my ($junk, $state, $city, $zip, $foo, $bar, $county, $baz, $morejunk) = split(/\|/);
		$zipdata{$zip} = [$city, $state, $county];
	}

	if (defined $zipdata{$input}) {
		my ($city, $state, $county) = @{$zipdata{$input}};
		return [$city, $state, $county];
	} elsif (defined $zipdata{substr $input, 0, 5}) {
		my ($city, $state, $county) = @{$zipdata{substr $input, 0, 5}};
		return [$city, $state, $county];
	} else {
		return ['ZIP not found', 'ZIP not found', 'ZIP not found'];
	}
  
$$ LANGUAGE PLPERLU STABLE;
