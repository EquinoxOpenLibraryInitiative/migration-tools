CREATE OR REPLACE FUNCTION migration_tools.name_parse_out_first_middle_last_comma_suffix (TEXT) RETURNS TEXT[] AS $$
    DECLARE
        full_name TEXT := $1;
        before_comma TEXT;
        family_name TEXT := '';
        first_given_name TEXT := '';
        second_given_name TEXT := '';
        suffix TEXT := '';
        prefix TEXT := '';
    BEGIN
        before_comma := BTRIM( REGEXP_REPLACE(full_name,E'^(.+),.+$',E'\\1') );
        suffix := CASE WHEN full_name ~ ',' THEN BTRIM( REGEXP_REPLACE(full_name,E'^.+,(.+)$',E'\\1') ) ELSE '' END;

        IF suffix = before_comma THEN
            suffix := '';
        END IF;

        family_name := BTRIM( REGEXP_REPLACE(before_comma,E'^.+\\s(.+)$',E'\\1') );
        first_given_name := BTRIM( REGEXP_REPLACE(before_comma,E'^(.+?)\\s.+$',E'\\1') );
        second_given_name := BTRIM( CASE WHEN before_comma ~ '^.+\s.+\s.+$' THEN REGEXP_REPLACE(before_comma,E'^.+\\s(.+)\\s.+$',E'\\1') ELSE '' END );

        RETURN ARRAY[ family_name, prefix, first_given_name, second_given_name, suffix ];
    END;
$$ LANGUAGE PLPGSQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION migration_tools.name_parse_out_last_comma_prefix_first_middle_suffix (TEXT) RETURNS TEXT[] AS $$
    DECLARE
        full_name TEXT := $1;
        temp TEXT;
        family_name TEXT := '';
        first_given_name TEXT := '';
        second_given_name TEXT := '';
        suffix TEXT := '';
        prefix TEXT := '';
    BEGIN
        temp := full_name;
        -- Use values, not structure, for prefix/suffix, unless we come up with a better idea
        IF temp ilike '%MR.%' THEN
            prefix := 'Mr.';
            temp := REGEXP_REPLACE( temp, E'MR\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%MRS.%' THEN
            prefix := 'Mrs.';
            temp := REGEXP_REPLACE( temp, E'MRS\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%MS.%' THEN
            prefix := 'Ms.';
            temp := REGEXP_REPLACE( temp, E'MS\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%DR.%' THEN
            prefix := 'Dr.';
            temp := REGEXP_REPLACE( temp, E'DR\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%JR%' THEN
            suffix := 'Jr.';
            temp := REGEXP_REPLACE( temp, E'JR\.?\\s*', '', 'i' );
        END IF;
        IF temp ilike '%JR,%' THEN
            suffix := 'Jr.';
            temp := REGEXP_REPLACE( temp, E'JR,\\s*', ',', 'i' );
        END IF;
        IF temp ilike '%SR%' THEN
            suffix := 'Sr.';
            temp := REGEXP_REPLACE( temp, E'SR\.?\\s*', '', 'i' );
        END IF;
        IF temp ilike '%SR,%' THEN
            suffix := 'Sr.';
            temp := REGEXP_REPLACE( temp, E'SR,\\s*', ',', 'i' );
        END IF;
        IF temp ~ E'\\sII$' THEN
            suffix := 'II';
            temp := REGEXP_REPLACE( temp, E'II$', '', 'i' );
        END IF;
        IF temp ~ E'\\sIII$' THEN
            suffix := 'III';
            temp := REGEXP_REPLACE( temp, E'III$', '', 'i' );
        END IF;
        IF temp ~ E'\\sIV$' THEN
            suffix := 'IV';
            temp := REGEXP_REPLACE( temp, E'IV$', '', 'i' );
        END IF;

        family_name := BTRIM( REGEXP_REPLACE(temp,E'^([^,]*)\\s*,.*$',E'\\1') );
        first_given_name := BTRIM( CASE WHEN temp ~ ',' THEN REGEXP_REPLACE(temp,E'^[^,]*\\s*,\\s*([^,\\s]*)\\s*.*$',E'\\1') ELSE 'N/A' END );
        second_given_name := BTRIM( CASE WHEN temp ~ ',' THEN REGEXP_REPLACE(temp,E'^[^,]*\\s*,\\s*[^,\\s]*\\s*(.*)$',E'\\1') ELSE ''  END );

        RETURN ARRAY[ family_name, prefix, first_given_name, second_given_name, suffix ];
    END;
$$ LANGUAGE PLPGSQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION migration_tools.name_parse_out_last_first_middle_and_random_affix (TEXT) RETURNS TEXT[] AS $$
    DECLARE
        full_name TEXT := $1;
        temp TEXT;
        family_name TEXT := '';
        first_given_name TEXT := '';
        second_given_name TEXT := '';
        suffix TEXT := '';
        prefix TEXT := '';
    BEGIN
        temp := full_name;
        -- Use values, not structure, for prefix/suffix, unless we come up with a better idea
        IF temp ilike '%MR.%' THEN
            prefix := 'Mr.';
            temp := REGEXP_REPLACE( temp, E'MR\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%MRS.%' THEN
            prefix := 'Mrs.';
            temp := REGEXP_REPLACE( temp, E'MRS\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%MS.%' THEN
            prefix := 'Ms.';
            temp := REGEXP_REPLACE( temp, E'MS\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%DR.%' THEN
            prefix := 'Dr.';
            temp := REGEXP_REPLACE( temp, E'DR\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%JR.%' THEN
            suffix := 'Jr.';
            temp := REGEXP_REPLACE( temp, E'JR\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%JR,%' THEN
            suffix := 'Jr.';
            temp := REGEXP_REPLACE( temp, E'JR,\\s*', ',', 'i' );
        END IF;
        IF temp ilike '%SR.%' THEN
            suffix := 'Sr.';
            temp := REGEXP_REPLACE( temp, E'SR\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%SR,%' THEN
            suffix := 'Sr.';
            temp := REGEXP_REPLACE( temp, E'SR,\\s*', ',', 'i' );
        END IF;
        IF temp like '%III%' THEN
            suffix := 'III';
            temp := REGEXP_REPLACE( temp, E'III', '' );
        END IF;
        IF temp like '%II%' THEN
            suffix := 'II';
            temp := REGEXP_REPLACE( temp, E'II', '' );
        END IF;
        IF temp like '%IV%' THEN
            suffix := 'IV';
            temp := REGEXP_REPLACE( temp, E'IV', '' );
        END IF;

        temp := REGEXP_REPLACE( temp, '\(\)', '');
        family_name := BTRIM( REGEXP_REPLACE(temp,E'^(\\S+).*$',E'\\1') );
        family_name := REGEXP_REPLACE( family_name, ',', '' );
        first_given_name := CASE WHEN temp ~ E'^\\S+$' THEN 'N/A' ELSE BTRIM( REGEXP_REPLACE(temp,E'^\\S+\\s+(\\S+).*$',E'\\1') ) END;
        first_given_name := REGEXP_REPLACE( first_given_name, ',', '' );
        second_given_name := CASE WHEN temp ~ E'^\\S+$' THEN '' ELSE BTRIM( REGEXP_REPLACE(temp,E'^\\S+\\s+\\S+\\s*(.*)$',E'\\1') ) END;
        second_given_name := REGEXP_REPLACE( second_given_name, ',', '' );

        RETURN ARRAY[ family_name, prefix, first_given_name, second_given_name, suffix ];
    END;
$$ LANGUAGE PLPGSQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION migration_tools.name_parse_out_fuller_last_first_middle_and_random_affix (TEXT) RETURNS TEXT[] AS $$
    DECLARE
        full_name TEXT := $1;
        temp TEXT;
        family_name TEXT := '';
        first_given_name TEXT := '';
        second_given_name TEXT := '';
        suffix TEXT := '';
        prefix TEXT := '';
    BEGIN
        temp := BTRIM(full_name);
        -- Use values, not structure, for prefix/suffix, unless we come up with a better idea
        --IF temp ~ '^\S{2,}\.' THEN
        --    prefix := REGEXP_REPLACE(temp, '^(\S{2,}\.).*$','\1');
        --    temp := BTRIM(REGEXP_REPLACE(temp, '^\S{2,}\.(.*)$','\1'));
        --END IF;
        --IF temp ~ '\S{2,}\.$' THEN
        --    suffix := REGEXP_REPLACE(temp, '^.*(\S{2,}\.)$','\1');
        --    temp := REGEXP_REPLACE(temp, '^(.*)\S{2,}\.$','\1');
        --END IF;
        IF temp ilike '%MR.%' THEN
            prefix := 'Mr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'MR\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%MRS.%' THEN
            prefix := 'Mrs.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'MRS\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%MS.%' THEN
            prefix := 'Ms.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'MS\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%DR.%' THEN
            prefix := 'Dr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'DR\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%JR.%' THEN
            suffix := 'Jr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'JR\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%JR,%' THEN
            suffix := 'Jr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'JR,\\s*', ',', 'i' ));
        END IF;
        IF temp ilike '%SR.%' THEN
            suffix := 'Sr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'SR\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%SR,%' THEN
            suffix := 'Sr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'SR,\\s*', ',', 'i' ));
        END IF;
        IF temp like '%III%' THEN
            suffix := 'III';
            temp := BTRIM(REGEXP_REPLACE( temp, E'III', '' ));
        END IF;
        IF temp like '%II%' THEN
            suffix := 'II';
            temp := BTRIM(REGEXP_REPLACE( temp, E'II', '' ));
        END IF;

        IF temp ~ ',' THEN
            family_name = BTRIM(REGEXP_REPLACE(temp,'^(.*?,).*$','\1'));
            temp := BTRIM(REPLACE( temp, family_name, '' ));
            family_name := REPLACE( family_name, ',', '' );
            IF temp ~ ' ' THEN
                first_given_name := BTRIM( REGEXP_REPLACE(temp,'^(.+)\s(.+)$','\1') );
                second_given_name := BTRIM( REGEXP_REPLACE(temp,'^(.+)\s(.+)$','\2') );
            ELSE
                first_given_name := temp;
                second_given_name := '';
            END IF;
        ELSE
            IF temp ~ '^\S+\s+\S+\s+\S+$' THEN
                first_given_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)\s*(\S+)$','\1') );
                second_given_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)\s*(\S+)$','\2') );
                family_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)\s*(\S+)$','\3') );
            ELSE
                first_given_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)$','\1') );
                second_given_name := temp;
                family_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)$','\2') );
            END IF;
        END IF;

        RETURN ARRAY[ family_name, prefix, first_given_name, second_given_name, suffix ];
    END;
$$ LANGUAGE PLPGSQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION migration_tools.name_parse_out_fuller_last_first_middle_and_random_affix2 (TEXT) RETURNS TEXT[] AS $$
    DECLARE
        full_name TEXT := $1;
        temp TEXT;
        family_name TEXT := '';
        first_given_name TEXT := '';
        second_given_name TEXT := '';
        suffix TEXT := '';
        prefix TEXT := '';
    BEGIN
        temp := BTRIM(full_name);
        -- Use values, not structure, for prefix/suffix, unless we come up with a better idea
        --IF temp ~ '^\S{2,}\.' THEN
        --    prefix := REGEXP_REPLACE(temp, '^(\S{2,}\.).*$','\1');
        --    temp := BTRIM(REGEXP_REPLACE(temp, '^\S{2,}\.(.*)$','\1'));
        --END IF;
        --IF temp ~ '\S{2,}\.$' THEN
        --    suffix := REGEXP_REPLACE(temp, '^.*(\S{2,}\.)$','\1');
        --    temp := REGEXP_REPLACE(temp, '^(.*)\S{2,}\.$','\1');
        --END IF;
        IF temp ilike '%MR.%' THEN
            prefix := 'Mr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'MR\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%MRS.%' THEN
            prefix := 'Mrs.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'MRS\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%MS.%' THEN
            prefix := 'Ms.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'MS\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%DR.%' THEN
            prefix := 'Dr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'DR\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%JR.%' THEN
            suffix := 'Jr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'JR\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%JR,%' THEN
            suffix := 'Jr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'JR,\\s*', ',', 'i' ));
        END IF;
        IF temp ilike '%SR.%' THEN
            suffix := 'Sr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'SR\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%SR,%' THEN
            suffix := 'Sr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'SR,\\s*', ',', 'i' ));
        END IF;
        IF temp like '%III%' THEN
            suffix := 'III';
            temp := BTRIM(REGEXP_REPLACE( temp, E'III', '' ));
        END IF;
        IF temp like '%II%' THEN
            suffix := 'II';
            temp := BTRIM(REGEXP_REPLACE( temp, E'II', '' ));
        END IF;

        IF temp ~ ',' THEN
            family_name = BTRIM(REGEXP_REPLACE(temp,'^(.*?,).*$','\1'));
            temp := BTRIM(REPLACE( temp, family_name, '' ));
            family_name := REPLACE( family_name, ',', '' );
            IF temp ~ ' ' THEN
                first_given_name := BTRIM( REGEXP_REPLACE(temp,'^(.+)\s(.+)$','\1') );
                second_given_name := BTRIM( REGEXP_REPLACE(temp,'^(.+)\s(.+)$','\2') );
            ELSE
                first_given_name := temp;
                second_given_name := '';
            END IF;
        ELSE
            IF temp ~ '^\S+\s+\S+\s+\S+$' THEN
                first_given_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)\s*(\S+)$','\1') );
                second_given_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)\s*(\S+)$','\2') );
                family_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)\s*(\S+)$','\3') );
            ELSE
                first_given_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)$','\1') );
                second_given_name := temp;
                family_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)$','\2') );
            END IF;
        END IF;

        family_name := BTRIM(REPLACE(REPLACE(family_name,',',''),'"',''));
        first_given_name := BTRIM(REPLACE(REPLACE(first_given_name,',',''),'"',''));
        second_given_name := BTRIM(REPLACE(REPLACE(second_given_name,',',''),'"',''));

        RETURN ARRAY[ family_name, prefix, first_given_name, second_given_name, suffix ];
    END;
$$ LANGUAGE PLPGSQL STRICT IMMUTABLE;

