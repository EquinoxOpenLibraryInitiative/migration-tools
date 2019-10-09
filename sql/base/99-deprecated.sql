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

CREATE OR REPLACE FUNCTION migration_tools.is_staff_profile (INT) RETURNS BOOLEAN AS $$
  DECLARE
    profile ALIAS FOR $1;
  BEGIN
    RETURN CASE WHEN 'Staff' IN (select (permission.grp_ancestors(profile)).name) THEN TRUE ELSE FALSE END;
  END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.base_item_dynamic_field_map (TEXT) RETURNS TEXT AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        output TEXT;
    BEGIN
        FOR output IN
            EXECUTE 'SELECT ''' || migration_schema || '.'' || value FROM ' || migration_schema || '.config WHERE key = ''base_item_dynamic_field_map'';'
        LOOP
            RETURN output;
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.base_copy_location_map (TEXT) RETURNS TEXT AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        output TEXT;
    BEGIN
        FOR output IN
            EXECUTE 'SELECT ''' || migration_schema || '.'' || value FROM ' || migration_schema || '.config WHERE key = ''base_copy_location_map'';'
        LOOP
            RETURN output;
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.base_circ_field_map (TEXT) RETURNS TEXT AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        output TEXT;
    BEGIN
        FOR output IN
            EXECUTE 'SELECT ''' || migration_schema || '.'' || value FROM ' || migration_schema || '.config WHERE key = ''base_circ_field_map'';'
        LOOP
            RETURN output;
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.map_base_patron_profile (TEXT,TEXT,INTEGER) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        profile_map TEXT;
        patron_table ALIAS FOR $2;
        default_patron_profile ALIAS FOR $3;
        sql TEXT;
        sql_update TEXT;
        sql_where1 TEXT := '';
        sql_where2 TEXT := '';
        sql_where3 TEXT := '';
        output RECORD;
    BEGIN
        SELECT migration_tools.base_profile_map(migration_schema) INTO STRICT profile_map;
        FOR output IN 
            EXECUTE 'SELECT * FROM ' || profile_map || E' ORDER BY id;'
        LOOP
            sql_update := 'UPDATE ' || patron_table || ' AS u SET profile = perm_grp_id FROM ' || profile_map || ' AS m WHERE ';
            sql_where1 := NULLIF(output.legacy_field1,'') || ' = ' || quote_literal( output.legacy_value1 ) || ' AND legacy_field1 = ' || quote_literal(output.legacy_field1) || ' AND legacy_value1 = ' || quote_literal(output.legacy_value1);
            sql_where2 := NULLIF(output.legacy_field2,'') || ' = ' || quote_literal( output.legacy_value2 ) || ' AND legacy_field2 = ' || quote_literal(output.legacy_field2) || ' AND legacy_value2 = ' || quote_literal(output.legacy_value2);
            sql_where3 := NULLIF(output.legacy_field3,'') || ' = ' || quote_literal( output.legacy_value3 ) || ' AND legacy_field3 = ' || quote_literal(output.legacy_field3) || ' AND legacy_value3 = ' || quote_literal(output.legacy_value3);
            sql := sql_update || COALESCE(sql_where1,'') || CASE WHEN sql_where1 <> '' AND sql_where2<> ''  THEN ' AND ' ELSE '' END || COALESCE(sql_where2,'') || CASE WHEN sql_where2 <> '' AND sql_where3 <> '' THEN ' AND ' ELSE '' END || COALESCE(sql_where3,'') || ';';
            --RAISE INFO 'sql = %', sql;
            PERFORM migration_tools.exec( $1, sql );
        END LOOP;
        PERFORM migration_tools.exec( $1, 'UPDATE ' || patron_table || ' AS u SET profile = ' || quote_literal(default_patron_profile) || ' WHERE profile IS NULL;'  );
        BEGIN
            PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''last_base_patron_mapping_profile'', now() );' );
        EXCEPTION
            WHEN OTHERS THEN PERFORM migration_tools.exec( $1, 'UPDATE ' || migration_schema || '.config SET value = now() WHERE key = ''last_base_patron_mapping_profile'';' );
        END;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.map_base_item_table_dynamic (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        field_map TEXT;
        item_table ALIAS FOR $2;
        sql TEXT;
        sql_update TEXT;
        sql_where1 TEXT := '';
        sql_where2 TEXT := '';
        sql_where3 TEXT := '';
        output RECORD;
    BEGIN
        SELECT migration_tools.base_item_dynamic_field_map(migration_schema) INTO STRICT field_map;
        FOR output IN 
            EXECUTE 'SELECT * FROM ' || field_map || E' ORDER BY id;'
        LOOP
            sql_update := 'UPDATE ' || item_table || ' AS i SET ' || output.evergreen_field || E' = ' || quote_literal(output.evergreen_value) || '::' || output.evergreen_datatype || E' FROM ' || field_map || ' AS m WHERE ';
            sql_where1 := NULLIF(output.legacy_field1,'') || ' = ' || quote_literal( output.legacy_value1 ) || ' AND legacy_field1 = ' || quote_literal(output.legacy_field1) || ' AND legacy_value1 = ' || quote_literal(output.legacy_value1);
            sql_where2 := NULLIF(output.legacy_field2,'') || ' = ' || quote_literal( output.legacy_value2 ) || ' AND legacy_field2 = ' || quote_literal(output.legacy_field2) || ' AND legacy_value2 = ' || quote_literal(output.legacy_value2);
            sql_where3 := NULLIF(output.legacy_field3,'') || ' = ' || quote_literal( output.legacy_value3 ) || ' AND legacy_field3 = ' || quote_literal(output.legacy_field3) || ' AND legacy_value3 = ' || quote_literal(output.legacy_value3);
            sql := sql_update || COALESCE(sql_where1,'') || CASE WHEN sql_where1 <> '' AND sql_where2<> ''  THEN ' AND ' ELSE '' END || COALESCE(sql_where2,'') || CASE WHEN sql_where2 <> '' AND sql_where3 <> '' THEN ' AND ' ELSE '' END || COALESCE(sql_where3,'') || ';';
            --RAISE INFO 'sql = %', sql;
            PERFORM migration_tools.exec( $1, sql );
        END LOOP;
        BEGIN
            PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''last_base_item_mapping_dynamic'', now() );' );
        EXCEPTION
            WHEN OTHERS THEN PERFORM migration_tools.exec( $1, 'UPDATE ' || migration_schema || '.config SET value = now() WHERE key = ''last_base_item_mapping_dynamic'';' );
        END;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.map_base_item_table_locations (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        base_copy_location_map TEXT;
        item_table ALIAS FOR $2;
        sql TEXT;
        sql_update TEXT;
        sql_where1 TEXT := '';
        sql_where2 TEXT := '';
        sql_where3 TEXT := '';
        output RECORD;
    BEGIN
        SELECT migration_tools.base_copy_location_map(migration_schema) INTO STRICT base_copy_location_map;
        FOR output IN 
            EXECUTE 'SELECT * FROM ' || base_copy_location_map || E' ORDER BY id;'
        LOOP
            sql_update := 'UPDATE ' || item_table || ' AS i SET location = m.location FROM ' || base_copy_location_map || ' AS m WHERE ';
            sql_where1 := NULLIF(output.legacy_field1,'') || ' = ' || quote_literal( output.legacy_value1 ) || ' AND legacy_field1 = ' || quote_literal(output.legacy_field1) || ' AND legacy_value1 = ' || quote_literal(output.legacy_value1);
            sql_where2 := NULLIF(output.legacy_field2,'') || ' = ' || quote_literal( output.legacy_value2 ) || ' AND legacy_field2 = ' || quote_literal(output.legacy_field2) || ' AND legacy_value2 = ' || quote_literal(output.legacy_value2);
            sql_where3 := NULLIF(output.legacy_field3,'') || ' = ' || quote_literal( output.legacy_value3 ) || ' AND legacy_field3 = ' || quote_literal(output.legacy_field3) || ' AND legacy_value3 = ' || quote_literal(output.legacy_value3);
            sql := sql_update || COALESCE(sql_where1,'') || CASE WHEN sql_where1 <> '' AND sql_where2<> ''  THEN ' AND ' ELSE '' END || COALESCE(sql_where2,'') || CASE WHEN sql_where2 <> '' AND sql_where3 <> '' THEN ' AND ' ELSE '' END || COALESCE(sql_where3,'') || ';';
            --RAISE INFO 'sql = %', sql;
            PERFORM migration_tools.exec( $1, sql );
        END LOOP;
        BEGIN
            PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''last_base_item_mapping_locations'', now() );' );
        EXCEPTION
            WHEN OTHERS THEN PERFORM migration_tools.exec( $1, 'UPDATE ' || migration_schema || '.config SET value = now() WHERE key = ''last_base_item_mapping_locations'';' );
        END;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- circulate       loan period     max renewals    max out fine amount     fine interval   max fine        item field 1    item value 1    item field 2    item value 2    patron field 1  patron value 1  patron field 2  patron value 2
CREATE OR REPLACE FUNCTION migration_tools.map_base_circ_table_dynamic (TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        field_map TEXT;
        circ_table ALIAS FOR $2;
        item_table ALIAS FOR $3;
        patron_table ALIAS FOR $4;
        sql TEXT;
        sql_update TEXT;
        sql_where1 TEXT := '';
        sql_where2 TEXT := '';
        sql_where3 TEXT := '';
        sql_where4 TEXT := '';
        output RECORD;
    BEGIN
        SELECT migration_tools.base_circ_field_map(migration_schema) INTO STRICT field_map;
        FOR output IN 
            EXECUTE 'SELECT * FROM ' || field_map || E' ORDER BY id;'
        LOOP
            sql_update := 'UPDATE ' || circ_table || ' AS c SET duration = ' || quote_literal(output.loan_period) || '::INTERVAL, renewal_remaining = ' || quote_literal(output.max_renewals) || '::INTEGER, recuring_fine = ' || quote_literal(output.fine_amount) || '::NUMERIC(6,2), fine_interval = ' || quote_literal(output.fine_interval) || '::INTERVAL, max_fine = ' || quote_literal(output.max_fine) || '::NUMERIC(6,2) FROM ' || field_map || ' AS m, ' || item_table || ' AS i, ' || patron_table || ' AS u WHERE c.usr = u.id AND c.target_copy = i.id AND ';
            sql_where1 := NULLIF(output.item_field1,'') || ' = ' || quote_literal( output.item_value1 ) || ' AND item_field1 = ' || quote_literal(output.item_field1) || ' AND item_value1 = ' || quote_literal(output.item_value1);
            sql_where2 := NULLIF(output.item_field2,'') || ' = ' || quote_literal( output.item_value2 ) || ' AND item_field2 = ' || quote_literal(output.item_field2) || ' AND item_value2 = ' || quote_literal(output.item_value2);
            sql_where3 := NULLIF(output.patron_field1,'') || ' = ' || quote_literal( output.patron_value1 ) || ' AND patron_field1 = ' || quote_literal(output.patron_field1) || ' AND patron_value1 = ' || quote_literal(output.patron_value1);
            sql_where4 := NULLIF(output.patron_field2,'') || ' = ' || quote_literal( output.patron_value2 ) || ' AND patron_field2 = ' || quote_literal(output.patron_field2) || ' AND patron_value2 = ' || quote_literal(output.patron_value2);
            sql := sql_update || COALESCE(sql_where1,'') || CASE WHEN sql_where1 <> '' AND sql_where2<> ''  THEN ' AND ' ELSE '' END || COALESCE(sql_where2,'') || CASE WHEN sql_where2 <> '' AND sql_where3 <> '' THEN ' AND ' ELSE '' END || COALESCE(sql_where3,'') || CASE WHEN sql_where3 <> '' AND sql_where4 <> '' THEN ' AND ' ELSE '' END || COALESCE(sql_where4,'') || ';';
            --RAISE INFO 'sql = %', sql;
            PERFORM migration_tools.exec( $1, sql );
        END LOOP;
        BEGIN
            PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''last_base_circ_field_mapping'', now() );' );
        EXCEPTION
            WHEN OTHERS THEN PERFORM migration_tools.exec( $1, 'UPDATE ' || migration_schema || '.config SET value = now() WHERE key = ''last_base_circ_field_mapping'';' );
        END;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.apply_circ_matrix_before_20( tablename TEXT ) RETURNS VOID AS $$

-- Usage:
--
--   First make sure the circ matrix is loaded and the circulations
--   have been staged to the extent possible (but at the very least
--   circ_lib, target_copy, usr, and *_renewal).  User profiles and
--   circ modifiers must also be in place.
--
--   SELECT migration_tools.apply_circ_matrix('m_pioneer.action_circulation');
--

DECLARE
  circ_lib             INT;
  target_copy          INT;
  usr                  INT;
  is_renewal           BOOLEAN;
  this_duration_rule   INT;
  this_fine_rule       INT;
  this_max_fine_rule   INT;
  rcd                  config.rule_circ_duration%ROWTYPE;
  rrf                  config.rule_recurring_fine%ROWTYPE;
  rmf                  config.rule_max_fine%ROWTYPE;
  circ                 INT;
  n                    INT := 0;
  n_circs              INT;
  
BEGIN

  EXECUTE 'SELECT COUNT(*) FROM ' || tablename || ';' INTO n_circs;

  FOR circ IN EXECUTE ('SELECT id FROM ' || tablename) LOOP

    -- Fetch the correct rules for this circulation
    EXECUTE ('
      SELECT
        circ_lib,
        target_copy,
        usr,
        CASE
          WHEN phone_renewal OR desk_renewal OR opac_renewal THEN TRUE
          ELSE FALSE
        END
      FROM ' || tablename || ' WHERE id = ' || circ || ';')
      INTO circ_lib, target_copy, usr, is_renewal ;
    SELECT
      INTO this_duration_rule,
           this_fine_rule,
           this_max_fine_rule
      duration_rule,
      recuring_fine_rule,
      max_fine_rule
      FROM action.find_circ_matrix_matchpoint(
        circ_lib,
        target_copy,
        usr,
        is_renewal
        );
    SELECT INTO rcd * FROM config.rule_circ_duration
      WHERE id = this_duration_rule;
    SELECT INTO rrf * FROM config.rule_recurring_fine
      WHERE id = this_fine_rule;
    SELECT INTO rmf * FROM config.rule_max_fine
      WHERE id = this_max_fine_rule;

    -- Apply the rules to this circulation
    EXECUTE ('UPDATE ' || tablename || ' c
    SET
      duration_rule = rcd.name,
      recuring_fine_rule = rrf.name,
      max_fine_rule = rmf.name,
      duration = rcd.normal,
      recuring_fine = rrf.normal,
      max_fine =
        CASE rmf.is_percent
          WHEN TRUE THEN (rmf.amount / 100.0) * ac.price
          ELSE rmf.amount
        END,
      renewal_remaining = rcd.max_renewals
    FROM
      config.rule_circ_duration rcd,
      config.rule_recuring_fine rrf,
      config.rule_max_fine rmf,
                        asset.copy ac
    WHERE
      rcd.id = ' || this_duration_rule || ' AND
      rrf.id = ' || this_fine_rule || ' AND
      rmf.id = ' || this_max_fine_rule || ' AND
                        ac.id = c.target_copy AND
      c.id = ' || circ || ';');

    -- Keep track of where we are in the process
    n := n + 1;
    IF (n % 100 = 0) THEN
      RAISE INFO '%', n || ' of ' || n_circs
        || ' (' || (100*n/n_circs) || '%) circs updated.';
    END IF;

  END LOOP;

  RETURN;
END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migration_tools.apply_circ_matrix_after_20( tablename TEXT ) RETURNS VOID AS $$

-- Usage:
--
--   First make sure the circ matrix is loaded and the circulations
--   have been staged to the extent possible (but at the very least
--   circ_lib, target_copy, usr, and *_renewal).  User profiles and
--   circ modifiers must also be in place.
--
--   SELECT migration_tools.apply_circ_matrix('m_pioneer.action_circulation');
--

DECLARE
  circ_lib             INT;
  target_copy          INT;
  usr                  INT;
  is_renewal           BOOLEAN;
  this_duration_rule   INT;
  this_fine_rule       INT;
  this_max_fine_rule   INT;
  rcd                  config.rule_circ_duration%ROWTYPE;
  rrf                  config.rule_recurring_fine%ROWTYPE;
  rmf                  config.rule_max_fine%ROWTYPE;
  circ                 INT;
  n                    INT := 0;
  n_circs              INT;
  
BEGIN

  EXECUTE 'SELECT COUNT(*) FROM ' || tablename || ';' INTO n_circs;

  FOR circ IN EXECUTE ('SELECT id FROM ' || tablename) LOOP

    -- Fetch the correct rules for this circulation
    EXECUTE ('
      SELECT
        circ_lib,
        target_copy,
        usr,
        CASE
          WHEN phone_renewal OR desk_renewal OR opac_renewal THEN TRUE
          ELSE FALSE
        END
      FROM ' || tablename || ' WHERE id = ' || circ || ';')
      INTO circ_lib, target_copy, usr, is_renewal ;
    SELECT
      INTO this_duration_rule,
           this_fine_rule,
           this_max_fine_rule
      (matchpoint).duration_rule,
      (matchpoint).recurring_fine_rule,
      (matchpoint).max_fine_rule
      FROM action.find_circ_matrix_matchpoint(
        circ_lib,
        target_copy,
        usr,
        is_renewal
        );
    SELECT INTO rcd * FROM config.rule_circ_duration
      WHERE id = this_duration_rule;
    SELECT INTO rrf * FROM config.rule_recurring_fine
      WHERE id = this_fine_rule;
    SELECT INTO rmf * FROM config.rule_max_fine
      WHERE id = this_max_fine_rule;

    -- Apply the rules to this circulation
    EXECUTE ('UPDATE ' || tablename || ' c
    SET
      duration_rule = rcd.name,
      recurring_fine_rule = rrf.name,
      max_fine_rule = rmf.name,
      duration = rcd.normal,
      recurring_fine = rrf.normal,
      max_fine =
        CASE rmf.is_percent
          WHEN TRUE THEN (rmf.amount / 100.0) * ac.price
          ELSE rmf.amount
        END,
      renewal_remaining = rcd.max_renewals,
      grace_period = rrf.grace_period
    FROM
      config.rule_circ_duration rcd,
      config.rule_recurring_fine rrf,
      config.rule_max_fine rmf,
                        asset.copy ac
    WHERE
      rcd.id = ' || this_duration_rule || ' AND
      rrf.id = ' || this_fine_rule || ' AND
      rmf.id = ' || this_max_fine_rule || ' AND
                        ac.id = c.target_copy AND
      c.id = ' || circ || ';');

    -- Keep track of where we are in the process
    n := n + 1;
    IF (n % 100 = 0) THEN
      RAISE INFO '%', n || ' of ' || n_circs
        || ' (' || (100*n/n_circs) || '%) circs updated.';
    END IF;

  END LOOP;

  RETURN;
END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migration_tools.insert_856_9_conditional (TEXT, TEXT) RETURNS TEXT AS $$

  ## USAGE: UPDATE biblio.record_entry SET marc = migration_tools.insert_856_9(marc, 'ABC') WHERE [...];

  my ($marcxml, $shortname) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;

  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');

    foreach my $field ( $marc->field('856') ) {
      if ( scalar(grep( /(contentreserve|netlibrary|overdrive)\.com/i, $field->subfield('u'))) > 0 &&
           ! ( $field->as_string('9') =~ m/$shortname/ ) ) {
        $field->add_subfields( '9' => $shortname );
				$field->update( ind2 => '0');
      }
    }

    $xml = $marc->as_xml_record;
    $xml =~ s/^<\?.+?\?>$//mo;
    $xml =~ s/\n//sgo;
    $xml =~ s/>\s+</></sgo;
  };

  return $xml;

$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.insert_856_9 (TEXT, TEXT) RETURNS TEXT AS $$

  ## USAGE: UPDATE biblio.record_entry SET marc = migration_tools.insert_856_9(marc, 'ABC') WHERE [...];

  my ($marcxml, $shortname) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;

  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');

    foreach my $field ( $marc->field('856') ) {
      if ( ! $field->as_string('9') ) {
        $field->add_subfields( '9' => $shortname );
      }
    }

    $xml = $marc->as_xml_record;
    $xml =~ s/^<\?.+?\?>$//mo;
    $xml =~ s/\n//sgo;
    $xml =~ s/>\s+</></sgo;
  };

  return $xml;

$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.refresh_opac_visible_copies ( ) RETURNS VOID AS $$

BEGIN	

	DELETE FROM asset.opac_visible_copies;

	INSERT INTO asset.opac_visible_copies (id, circ_lib, record)
		SELECT DISTINCT
			cp.id, cp.circ_lib, cn.record
		FROM
			asset.copy cp
			JOIN asset.call_number cn ON (cn.id = cp.call_number)
			JOIN actor.org_unit a ON (cp.circ_lib = a.id)
			JOIN asset.copy_location cl ON (cp.location = cl.id)
			JOIN config.copy_status cs ON (cp.status = cs.id)
			JOIN biblio.record_entry b ON (cn.record = b.id)
		WHERE 
			NOT cp.deleted AND
			NOT cn.deleted AND
			NOT b.deleted AND
			cs.opac_visible AND
			cl.opac_visible AND
			cp.opac_visible AND
			a.opac_visible AND
			cp.id NOT IN (SELECT id FROM asset.opac_visible_copies);

END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migration_tools.log (TEXT,TEXT,INTEGER) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        sql ALIAS FOR $2;
        nrows ALIAS FOR $3;
    BEGIN
        EXECUTE 'INSERT INTO ' || migration_schema || '.sql_log ( sql, row_count ) VALUES ( ' || quote_literal(sql) || ', ' || nrows || ' );';
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.exec (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        sql ALIAS FOR $2;
        nrows INTEGER;
    BEGIN
        EXECUTE 'UPDATE ' || migration_schema || '.sql_current SET sql = ' || quote_literal(sql) || ';';
        --RAISE INFO '%', sql;
        EXECUTE sql;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        PERFORM migration_tools.log(migration_schema,sql,nrows);
    EXCEPTION
        WHEN OTHERS THEN 
            RAISE EXCEPTION '!!!!!!!!!!! state = %, msg = %, sql = %', SQLSTATE, SQLERRM, sql;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.debug_exec (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        sql ALIAS FOR $2;
        nrows INTEGER;
    BEGIN
        EXECUTE 'UPDATE ' || migration_schema || '.sql_current SET sql = ' || quote_literal(sql) || ';';
        RAISE INFO 'debug_exec sql = %', sql;
        EXECUTE sql;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        PERFORM migration_tools.log(migration_schema,sql,nrows);
    EXCEPTION
        WHEN OTHERS THEN 
            RAISE EXCEPTION '!!!!!!!!!!! state = %, msg = %, sql = %', SQLSTATE, SQLERRM, sql;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.insert_base_into_production (TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        production_tables TEXT[];
    BEGIN
        --RAISE INFO 'In migration_tools.insert_into_production(%)', migration_schema;
        SELECT migration_tools.production_tables(migration_schema) INTO STRICT production_tables;
        FOR i IN array_lower(production_tables,1) .. array_upper(production_tables,1) LOOP
            PERFORM migration_tools.insert_into_production(migration_schema,production_tables[i]);
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.insert_into_production (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        production_table ALIAS FOR $2;
        base_staging_table TEXT;
        columns RECORD;
    BEGIN
        base_staging_table = REPLACE( production_table, '.', '_' );
        --RAISE INFO 'In migration_tools.insert_into_production(%,%) -> %', migration_schema, production_table, base_staging_table;
        EXECUTE 'INSERT INTO ' || production_table || ' SELECT * FROM ' || migration_schema || '.' || base_staging_table || ';';
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- push bib sequence and return starting value for reserved range
CREATE OR REPLACE FUNCTION migration_tools.push_bib_sequence(INTEGER) RETURNS BIGINT AS $$
    DECLARE
        bib_count ALIAS FOR $1;
        output BIGINT;
    BEGIN
        PERFORM setval('biblio.record_entry_id_seq',(SELECT MAX(id) FROM biblio.record_entry) + bib_count + 2000);
        FOR output IN
            SELECT CEIL(MAX(id)/1000)*1000+1000 FROM biblio.record_entry WHERE id < (SELECT last_value FROM biblio.record_entry_id_seq)
        LOOP
            RETURN output;
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for linking two tables
-- e.g. select migration_tools.handle_link(:'migschema','asset_copy','barcode','test_foo','l_barcode','x_acp_id',false);
CREATE OR REPLACE FUNCTION migration_tools.handle_link (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,BOOLEAN) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_x ALIAS FOR $6;
        btrim_desired ALIAS FOR $7;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_a, column_a;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_a, column_a; 
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_b, column_b;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_b, column_b; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_b)
            || ' DROP COLUMN IF EXISTS ' || quote_ident(column_x);
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_b)
            || ' ADD COLUMN ' || quote_ident(column_x) || ' BIGINT';

        IF btrim_desired THEN
            EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
                || ' SET ' || quote_ident(column_x) || ' = a.id FROM ' || quote_ident(table_a) || ' a'
                || ' WHERE BTRIM(a.' || quote_ident(column_a)
                || ') = BTRIM(b.' || quote_ident(column_b) || ')';
        ELSE
            EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
                || ' SET ' || quote_ident(column_x) || ' = a.id FROM ' || quote_ident(table_a) || ' a'
                || ' WHERE a.' || quote_ident(column_a)
                || ' = b.' || quote_ident(column_b);
        END IF;

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for linking two tables, but copying column w into column x instead of "id"
-- e.g. select migration_tools.handle_link2(:'migschema','asset_copy','barcode','test_foo','l_barcode','id','x_acp_id',false);
CREATE OR REPLACE FUNCTION migration_tools.handle_link2 (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,BOOLEAN) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_w ALIAS FOR $6;
        column_x ALIAS FOR $7;
        btrim_desired ALIAS FOR $8;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_a, column_a;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_a, column_a; 
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_b, column_b;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_b, column_b; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_b)
            || ' DROP COLUMN IF EXISTS ' || quote_ident(column_x);
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_b)
            || ' ADD COLUMN ' || quote_ident(column_x) || ' TEXT';

        IF btrim_desired THEN
            EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
                || ' SET ' || quote_ident(column_x) || ' = a.' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
                || ' WHERE BTRIM(a.' || quote_ident(column_a)
                || ') = BTRIM(b.' || quote_ident(column_b) || ')';
        ELSE
            EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
                || ' SET ' || quote_ident(column_x) || ' = a.' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
                || ' WHERE a.' || quote_ident(column_a)
                || ' = b.' || quote_ident(column_b);
        END IF;

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for linking two tables, but copying column w into column x instead of "id". Unlike handle_link2, this one won't drop the target column, and it also doesn't have a final boolean argument for btrim
-- e.g. select migration_tools.handle_link3(:'migschema','asset_copy','barcode','test_foo','l_barcode','id','x_acp_id');
CREATE OR REPLACE FUNCTION migration_tools.handle_link3 (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_w ALIAS FOR $6;
        column_x ALIAS FOR $7;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_a, column_a;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_a, column_a; 
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_b, column_b;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_b, column_b; 
        END IF;

        EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
            || ' SET ' || quote_ident(column_x) || ' = a.' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
            || ' WHERE a.' || quote_ident(column_a)
            || ' = b.' || quote_ident(column_b);

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.handle_link3_skip_null_or_empty_string (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_w ALIAS FOR $6;
        column_x ALIAS FOR $7;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_a, column_a;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_a, column_a; 
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_b, column_b;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_b, column_b; 
        END IF;

        EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
            || ' SET ' || quote_ident(column_x) || ' = a.' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
            || ' WHERE a.' || quote_ident(column_a)
            || ' = b.' || quote_ident(column_b)
            || ' AND NULLIF(a.' || quote_ident(column_w) || ','''') IS NOT NULL';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.handle_link3_skip_null (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_w ALIAS FOR $6;
        column_x ALIAS FOR $7;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_a, column_a;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_a, column_a; 
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_b, column_b;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_b, column_b; 
        END IF;

        EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
            || ' SET ' || quote_ident(column_x) || ' = a.' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
            || ' WHERE a.' || quote_ident(column_a)
            || ' = b.' || quote_ident(column_b)
            || ' AND a.' || quote_ident(column_w) || ' IS NOT NULL';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.handle_link3_skip_true (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_w ALIAS FOR $6;
        column_x ALIAS FOR $7;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_a, column_a;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_a, column_a; 
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_b, column_b;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_b, column_b; 
        END IF;

        EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
            || ' SET ' || quote_ident(column_x) || ' = a.' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
            || ' WHERE a.' || quote_ident(column_a)
            || ' = b.' || quote_ident(column_b)
            || ' AND a.' || quote_ident(column_w) || ' IS NOT TRUE';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.handle_link3_skip_false (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_w ALIAS FOR $6;
        column_x ALIAS FOR $7;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_a, column_a;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_a, column_a; 
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_b, column_b;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_b, column_b; 
        END IF;

        EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
            || ' SET ' || quote_ident(column_x) || ' = a.' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
            || ' WHERE a.' || quote_ident(column_a)
            || ' = b.' || quote_ident(column_b)
            || ' AND a.' || quote_ident(column_w) || ' IS NOT FALSE';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.handle_link3_concat_skip_null (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_w ALIAS FOR $6;
        column_x ALIAS FOR $7;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_a, column_a;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_a, column_a; 
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_b, column_b;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_b, column_b; 
        END IF;

        EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
            || ' SET ' || quote_ident(column_x) || ' = CONCAT_WS('' ; '',b.' || quote_ident(column_x) || ',a.' || quote_ident(column_w) || ') FROM ' || quote_ident(table_a) || ' a'
            || ' WHERE a.' || quote_ident(column_a)
            || ' = b.' || quote_ident(column_b)
            || ' AND NULLIF(a.' || quote_ident(column_w) || ','''') IS NOT NULL';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;
