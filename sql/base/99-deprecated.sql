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

CREATE OR REPLACE FUNCTION migration_tools.set_leader (TEXT, INT, TEXT) RETURNS TEXT AS $$
  my ($marcxml, $pos, $value) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;
  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
    my $leader = $marc->leader();
    substr($leader, $pos, 1) = $value;
    $marc->leader($leader);
    $xml = $marc->as_xml_record;
    $xml =~ s/^<\?.+?\?>$//mo;
    $xml =~ s/\n//sgo;
    $xml =~ s/>\s+</></sgo;
  };
  return $xml;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.set_008 (TEXT, INT, TEXT) RETURNS TEXT AS $$
  my ($marcxml, $pos, $value) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;
  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
    my $f008 = $marc->field('008');

    if ($f008) {
       my $field = $f008->data();
       substr($field, $pos, 1) = $value;
       $f008->update($field);
       $xml = $marc->as_xml_record;
       $xml =~ s/^<\?.+?\?>$//mo;
       $xml =~ s/\n//sgo;
       $xml =~ s/>\s+</></sgo;
    }
  };
  return $xml;
$$ LANGUAGE PLPERLU STABLE;


CREATE OR REPLACE FUNCTION migration_tools.is_staff_profile (INT) RETURNS BOOLEAN AS $$
  DECLARE
    profile ALIAS FOR $1;
  BEGIN
    RETURN CASE WHEN 'Staff' IN (select (permission.grp_ancestors(profile)).name) THEN TRUE ELSE FALSE END;
  END;
$$ LANGUAGE PLPGSQL STRICT STABLE;


CREATE OR REPLACE FUNCTION migration_tools.is_blank (TEXT) RETURNS BOOLEAN AS $$
  BEGIN
    RETURN CASE WHEN $1 = '' THEN TRUE ELSE FALSE END;
  END;
$$ LANGUAGE PLPGSQL STRICT STABLE;


CREATE OR REPLACE FUNCTION migration_tools.insert_tags (TEXT, TEXT) RETURNS TEXT AS $$

  my ($marcxml, $tags) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;

  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
    my $to_insert = MARC::Record->new_from_xml("<record>$tags</record>", 'UTF-8');

    my @incumbents = ();

    foreach my $field ( $marc->fields() ) {
      push @incumbents, $field->as_formatted();
    }

    foreach $field ( $to_insert->fields() ) {
      if (!grep {$_ eq $field->as_formatted()} @incumbents) {
        $marc->insert_fields_ordered( ($field) );
      }
    }

    $xml = $marc->as_xml_record;
    $xml =~ s/^<\?.+?\?>$//mo;
    $xml =~ s/\n//sgo;
    $xml =~ s/>\s+</></sgo;
  };

  return $xml;

$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.apply_circ_matrix( tablename TEXT ) RETURNS VOID AS $$

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
      recurring_fine_rule,
      max_fine_rule
      FROM action.item_user_circ_test(
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
      renewal_remaining = rcd.max_renewals
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

-- TODO: make another version of the procedure below that can work with specified copy staging tables
-- The following should track the logic of OpenILS::Application::AppUtils::get_copy_price
CREATE OR REPLACE FUNCTION migration_tools.get_copy_price( item BIGINT ) RETURNS NUMERIC AS $$
DECLARE
    context_lib             INT;
    charge_lost_on_zero     BOOLEAN;
    min_price               NUMERIC;
    max_price               NUMERIC;
    default_price           NUMERIC;
    working_price           NUMERIC;

BEGIN

    SELECT INTO context_lib CASE WHEN call_number = -1 THEN circ_lib ELSE owning_lib END
        FROM asset.copy ac, asset.call_number acn WHERE ac.call_number = acn.id AND ac.id = item;

    SELECT INTO charge_lost_on_zero value
        FROM actor.org_unit_ancestor_setting('circ.charge_lost_on_zero',context_lib);

    SELECT INTO min_price value
        FROM actor.org_unit_ancestor_setting('circ.min_item_price',context_lib);

    SELECT INTO max_price value
        FROM actor.org_unit_ancestor_setting('circ.max_item_price',context_lib);

    SELECT INTO default_price value
        FROM actor.org_unit_ancestor_setting('cat.default_item_price',context_lib);

    SELECT INTO working_price price FROM asset.copy WHERE id = item;

    IF (working_price IS NULL OR (working_price = 0 AND charge_lost_on_zero)) THEN
        working_price := default_price;
    END IF;

    IF (max_price IS NOT NULL AND working_price > max_price) THEN
        working_price := max_price;
    END IF;

    IF (min_price IS NOT NULL AND working_price < min_price) THEN
        IF (working_price <> 0 OR charge_lost_on_zero IS NULL OR charge_lost_on_zero) THEN
            working_price := min_price;
        END IF;
    END IF;

    RETURN working_price;

END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migration_tools.apply_circ_matrix_to_specific_circ( tablename TEXT, circ BIGINT ) RETURNS VOID AS $$

-- Usage:
--
--   First make sure the circ matrix is loaded and the circulations
--   have been staged to the extent possible (but at the very least
--   circ_lib, target_copy, usr, and *_renewal).  User profiles and
--   circ modifiers must also be in place.
--
--   SELECT migration_tools.apply_circ_matrix_to_specific_circ('m_nwrl.action_circulation', 18391960);
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
  n                    INT := 0;
  n_circs              INT := 1;
  
BEGIN

  --EXECUTE 'SELECT COUNT(*) FROM ' || tablename || ';' INTO n_circs;

  --FOR circ IN EXECUTE ('SELECT id FROM ' || tablename) LOOP

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
          WHEN TRUE THEN (rmf.amount / 100.0) * migration_tools.get_copy_price(ac.id)
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

  --END LOOP;

  RETURN;
END;

$$ LANGUAGE plpgsql;




CREATE OR REPLACE FUNCTION migration_tools.stage_not_applicable_asset_stat_cats( schemaname TEXT ) RETURNS VOID AS $$

-- USAGE: Make sure the stat_cat and stat_cat_entry tables are populated, including exactly one 'Not Applicable' entry per stat cat.
--        Then SELECT migration_tools.stage_not_applicable_asset_stat_cats('m_foo');

-- TODO: Make a variant that will go directly to production tables -- which would be useful for retrofixing the absence of N/A cats.
-- TODO: Add a similar tool for actor stat cats, which behave differently.

DECLARE
	c                    TEXT := schemaname || '.asset_copy_legacy';
	sc									 TEXT := schemaname || '.asset_stat_cat';
	sce									 TEXT := schemaname || '.asset_stat_cat_entry';
	scecm								 TEXT := schemaname || '.asset_stat_cat_entry_copy_map';
	stat_cat						 INT;
  stat_cat_entry       INT;
  
BEGIN

  FOR stat_cat IN EXECUTE ('SELECT id FROM ' || sc) LOOP

		EXECUTE ('SELECT id FROM ' || sce || ' WHERE stat_cat = ' || stat_cat || E' AND value = \'Not Applicable\';') INTO stat_cat_entry;

		EXECUTE ('INSERT INTO ' || scecm || ' (owning_copy, stat_cat, stat_cat_entry)
							SELECT c.id, ' || stat_cat || ', ' || stat_cat_entry || ' FROM ' || c || ' c WHERE c.id NOT IN
							(SELECT owning_copy FROM ' || scecm || ' WHERE stat_cat = ' || stat_cat || ');');

  END LOOP;

  RETURN;
END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migration_tools.assign_standing_penalties ( ) RETURNS VOID AS $$

-- USAGE: Once circulation data has been loaded, and group penalty thresholds have been set up, run this.
--        This will assign standing penalties as needed.

DECLARE
  org_unit  INT;
  usr       INT;

BEGIN

  FOR org_unit IN EXECUTE ('SELECT DISTINCT org_unit FROM permission.grp_penalty_threshold;') LOOP

    FOR usr IN EXECUTE ('SELECT id FROM actor.usr WHERE NOT deleted;') LOOP
  
      EXECUTE('SELECT actor.calculate_system_penalties(' || usr || ', ' || org_unit || ');');

    END LOOP;

  END LOOP;

  RETURN;

END;

$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION migration_tools.insert_metarecords_for_pristine_database () RETURNS VOID AS $$

BEGIN
  INSERT INTO metabib.metarecord (fingerprint, master_record)
    SELECT  DISTINCT ON (b.fingerprint) b.fingerprint, b.id
      FROM  biblio.record_entry b
      WHERE NOT b.deleted
        AND b.id IN (SELECT r.id FROM biblio.record_entry r LEFT JOIN metabib.metarecord_source_map k ON (k.source = r.id) WHERE k.id IS NULL AND r.fingerprint IS NOT NULL)
        AND NOT EXISTS ( SELECT 1 FROM metabib.metarecord WHERE fingerprint = b.fingerprint )
      ORDER BY b.fingerprint, b.quality DESC;
  INSERT INTO metabib.metarecord_source_map (metarecord, source)
    SELECT  m.id, r.id
      FROM  biblio.record_entry r
      JOIN  metabib.metarecord m USING (fingerprint)
     WHERE  NOT r.deleted;
END;
  
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION migration_tools.insert_metarecords_for_incumbent_database () RETURNS VOID AS $$

BEGIN
  INSERT INTO metabib.metarecord (fingerprint, master_record)
    SELECT  DISTINCT ON (b.fingerprint) b.fingerprint, b.id
      FROM  biblio.record_entry b
      WHERE NOT b.deleted
        AND b.id IN (SELECT r.id FROM biblio.record_entry r LEFT JOIN metabib.metarecord_source_map k ON (k.source = r.id) WHERE k.id IS NULL AND r.fingerprint IS NOT NULL)
        AND NOT EXISTS ( SELECT 1 FROM metabib.metarecord WHERE fingerprint = b.fingerprint )
      ORDER BY b.fingerprint, b.quality DESC;
  INSERT INTO metabib.metarecord_source_map (metarecord, source)
    SELECT  m.id, r.id
      FROM  biblio.record_entry r
        JOIN metabib.metarecord m USING (fingerprint)
      WHERE NOT r.deleted
        AND r.id IN (SELECT b.id FROM biblio.record_entry b LEFT JOIN metabib.metarecord_source_map k ON (k.source = b.id) WHERE k.id IS NULL);
END;
    
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION migration_tools.create_cards( schemaname TEXT ) RETURNS VOID AS $$

-- USAGE: Make sure the patrons are staged in schemaname.actor_usr_legacy and have 'usrname' assigned.
--        Then SELECT migration_tools.create_cards('m_foo');

DECLARE
	u                    TEXT := schemaname || '.actor_usr_legacy';
	c                    TEXT := schemaname || '.actor_card';
  
BEGIN

	EXECUTE ('DELETE FROM ' || c || ';');
	EXECUTE ('INSERT INTO ' || c || ' (usr, barcode) SELECT id, usrname FROM ' || u || ';');
	EXECUTE ('UPDATE ' || u || ' u SET card = c.id FROM ' || c || ' c WHERE c.usr = u.id;');

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


CREATE OR REPLACE FUNCTION migration_tools.change_call_number(copy_id BIGINT, new_label TEXT, cn_class BIGINT) RETURNS VOID AS $$

DECLARE
  old_volume   BIGINT;
  new_volume   BIGINT;
  bib          BIGINT;
  owner        INTEGER;
  old_label    TEXT;
  remainder    BIGINT;

BEGIN

  -- Bail out if asked to change the label to ##URI##
  IF new_label = '##URI##' THEN
    RETURN;
  END IF;

  -- Gather information
  SELECT call_number INTO old_volume FROM asset.copy WHERE id = copy_id;
  SELECT record INTO bib FROM asset.call_number WHERE id = old_volume;
  SELECT owning_lib, label INTO owner, old_label FROM asset.call_number WHERE id = old_volume;

  -- Bail out if the label already is ##URI##
  IF old_label = '##URI##' THEN
    RETURN;
  END IF;

  -- Bail out if the call number label is already correct
  IF new_volume = old_volume THEN
    RETURN;
  END IF;

  -- Check whether we already have a destination volume available
  SELECT id INTO new_volume FROM asset.call_number 
    WHERE 
      record = bib AND
      owning_lib = owner AND
      label = new_label AND
      NOT deleted;

  -- Create destination volume if needed
  IF NOT FOUND THEN
    INSERT INTO asset.call_number (creator, editor, record, owning_lib, label, label_class) 
      VALUES (1, 1, bib, owner, new_label, cn_class);
    SELECT id INTO new_volume FROM asset.call_number
      WHERE 
        record = bib AND
        owning_lib = owner AND
        label = new_label AND
        NOT deleted;
  END IF;

  -- Move copy to destination
  UPDATE asset.copy SET call_number = new_volume WHERE id = copy_id;

  -- Delete source volume if it is now empty
  SELECT id INTO remainder FROM asset.copy WHERE call_number = old_volume AND NOT deleted;
  IF NOT FOUND THEN
    DELETE FROM asset.call_number WHERE id = old_volume;
  END IF;

END;

$$ LANGUAGE plpgsql;

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

CREATE OR REPLACE FUNCTION migration_tools.check_ou_depth ( ) RETURNS VOID AS $$

DECLARE
  ou  INT;
	org_unit_depth INT;
	ou_parent INT;
	parent_depth INT;
  errors_found BOOLEAN;
	ou_shortname TEXT;
	parent_shortname TEXT;
	ou_type_name TEXT;
	parent_type TEXT;
	type_id INT;
	type_depth INT;
	type_parent INT;
	type_parent_depth INT;
	proper_parent TEXT;

BEGIN

	errors_found := FALSE;

-- Checking actor.org_unit_type

	FOR type_id IN EXECUTE ('SELECT id FROM actor.org_unit_type ORDER BY id;') LOOP

		SELECT depth FROM actor.org_unit_type WHERE id = type_id INTO type_depth;
		SELECT parent FROM actor.org_unit_type WHERE id = type_id INTO type_parent;

		IF type_parent IS NOT NULL THEN

			SELECT depth FROM actor.org_unit_type WHERE id = type_parent INTO type_parent_depth;

			IF type_depth - type_parent_depth <> 1 THEN
				SELECT name FROM actor.org_unit_type WHERE id = type_id INTO ou_type_name;
				SELECT name FROM actor.org_unit_type WHERE id = type_parent INTO parent_type;
				RAISE INFO 'The % org unit type has a depth of %, but its parent org unit type, %, has a depth of %.',
					ou_type_name, type_depth, parent_type, type_parent_depth;
				errors_found := TRUE;

			END IF;

		END IF;

	END LOOP;

-- Checking actor.org_unit

  FOR ou IN EXECUTE ('SELECT id FROM actor.org_unit ORDER BY shortname;') LOOP

		SELECT parent_ou FROM actor.org_unit WHERE id = ou INTO ou_parent;
		SELECT t.depth FROM actor.org_unit_type t, actor.org_unit o WHERE o.ou_type = t.id and o.id = ou INTO org_unit_depth;
		SELECT t.depth FROM actor.org_unit_type t, actor.org_unit o WHERE o.ou_type = t.id and o.id = ou_parent INTO parent_depth;
		SELECT shortname FROM actor.org_unit WHERE id = ou INTO ou_shortname;
		SELECT shortname FROM actor.org_unit WHERE id = ou_parent INTO parent_shortname;
		SELECT t.name FROM actor.org_unit_type t, actor.org_unit o WHERE o.ou_type = t.id and o.id = ou INTO ou_type_name;
		SELECT t.name FROM actor.org_unit_type t, actor.org_unit o WHERE o.ou_type = t.id and o.id = ou_parent INTO parent_type;

		IF ou_parent IS NOT NULL THEN

			IF	(org_unit_depth - parent_depth <> 1) OR (
				(SELECT parent FROM actor.org_unit_type WHERE name = ou_type_name) <> (SELECT id FROM actor.org_unit_type WHERE name = parent_type)
			) THEN
				RAISE INFO '% (org unit %) is a % (depth %) but its parent, % (org unit %), is a % (depth %).', 
					ou_shortname, ou, ou_type_name, org_unit_depth, parent_shortname, ou_parent, parent_type, parent_depth;
				errors_found := TRUE;
			END IF;

		END IF;

  END LOOP;

	IF NOT errors_found THEN
		RAISE INFO 'No errors found.';
	END IF;

  RETURN;

END;

$$ LANGUAGE plpgsql;


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


CREATE OR REPLACE FUNCTION migration_tools.change_owning_lib(copy_id BIGINT, new_owning_lib INTEGER) RETURNS VOID AS $$

DECLARE
  old_volume     BIGINT;
  new_volume     BIGINT;
  bib            BIGINT;
  old_owning_lib INTEGER;
	old_label      TEXT;
  remainder      BIGINT;

BEGIN

  -- Gather information
  SELECT call_number INTO old_volume FROM asset.copy WHERE id = copy_id;
  SELECT record INTO bib FROM asset.call_number WHERE id = old_volume;
  SELECT owning_lib, label INTO old_owning_lib, old_label FROM asset.call_number WHERE id = old_volume;

	-- Bail out if the new_owning_lib is not the ID of an org_unit
	IF new_owning_lib NOT IN (SELECT id FROM actor.org_unit) THEN
		RAISE WARNING 
			'% is not a valid actor.org_unit ID; no change made.', 
				new_owning_lib;
		RETURN;
	END IF;

  -- Bail out discreetly if the owning_lib is already correct
  IF new_owning_lib = old_owning_lib THEN
    RETURN;
  END IF;

  -- Check whether we already have a destination volume available
  SELECT id INTO new_volume FROM asset.call_number 
    WHERE 
      record = bib AND
      owning_lib = new_owning_lib AND
      label = old_label AND
      NOT deleted;

  -- Create destination volume if needed
  IF NOT FOUND THEN
    INSERT INTO asset.call_number (creator, editor, record, owning_lib, label) 
      VALUES (1, 1, bib, new_owning_lib, old_label);
    SELECT id INTO new_volume FROM asset.call_number
      WHERE 
        record = bib AND
        owning_lib = new_owning_lib AND
        label = old_label AND
        NOT deleted;
  END IF;

  -- Move copy to destination
  UPDATE asset.copy SET call_number = new_volume WHERE id = copy_id;

  -- Delete source volume if it is now empty
  SELECT id INTO remainder FROM asset.copy WHERE call_number = old_volume AND NOT deleted;
  IF NOT FOUND THEN
    DELETE FROM asset.call_number WHERE id = old_volume;
  END IF;

END;

$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION migration_tools.change_owning_lib(copy_id BIGINT, new_owner TEXT) RETURNS VOID AS $$

-- You can use shortnames with this function, which looks up the org unit ID and passes it to change_owning_lib(BIGINT,INTEGER).

DECLARE
	new_owning_lib	INTEGER;

BEGIN

	-- Parse the new_owner as an org unit ID or shortname
	IF new_owner IN (SELECT shortname FROM actor.org_unit) THEN
		SELECT id INTO new_owning_lib FROM actor.org_unit WHERE shortname = new_owner;
		PERFORM migration_tools.change_owning_lib(copy_id, new_owning_lib);
	ELSIF new_owner ~ E'^[0-9]+$' THEN
		IF new_owner::INTEGER IN (SELECT id FROM actor.org_unit) THEN
			RAISE INFO 
				'%',
				E'You don\'t need to put the actor.org_unit ID in quotes; '
					|| E'if you put it in quotes, I\'m going to try to parse it as a shortname first.';
			new_owning_lib := new_owner::INTEGER;
		PERFORM migration_tools.change_owning_lib(copy_id, new_owning_lib);
		END IF;
	ELSE
		RAISE WARNING 
			'% is not a valid actor.org_unit shortname or ID; no change made.', 
			new_owning_lib;
		RETURN;
	END IF;

END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migration_tools.marc_parses( TEXT ) RETURNS BOOLEAN AS $func$

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;

MARC::Charset->assume_unicode(1);

my $xml = shift;

eval {
    my $r = MARC::Record->new_from_xml( $xml );
    my $output_xml = $r->as_xml_record();
};
if ($@) {
    return 0;
} else {
    return 1;
}

$func$ LANGUAGE PLPERLU;
COMMENT ON FUNCTION migration_tools.marc_parses(TEXT) IS 'Return boolean indicating if MARCXML string is parseable by MARC::File::XML';

CREATE OR REPLACE FUNCTION migration_tools.simple_export_library_config(dir TEXT, orgs INT[]) RETURNS VOID AS $FUNC$
BEGIN
   EXECUTE $$COPY (SELECT * FROM actor.hours_of_operation WHERE id IN ($$ ||
           ARRAY_TO_STRING(orgs, ',') || $$)$$ ||
           $$) TO '$$ ||  dir || $$/actor_hours_of_operation'$$;
   EXECUTE $$COPY (SELECT org_unit, close_start, close_end, reason FROM actor.org_unit_closed WHERE org_unit IN ($$ ||
           ARRAY_TO_STRING(orgs, ',') || $$)$$ ||
           $$) TO '$$ ||  dir || $$/actor_org_unit_closed'$$;
   EXECUTE $$COPY (SELECT org_unit, name, value FROM actor.org_unit_setting WHERE org_unit IN ($$ ||
           ARRAY_TO_STRING(orgs, ',') || $$)$$ ||
           $$) TO '$$ ||  dir || $$/actor_org_unit_setting'$$;
   EXECUTE $$COPY (SELECT name, owning_lib, holdable, hold_verify, opac_visible, circulate FROM asset.copy_location WHERE owning_lib IN ($$ ||
           ARRAY_TO_STRING(orgs, ',') || $$)$$ ||
           $$) TO '$$ ||  dir || $$/asset_copy_location'$$;
   EXECUTE $$COPY (SELECT grp, org_unit, penalty, threshold FROM permission.grp_penalty_threshold WHERE org_unit IN ($$ ||
           ARRAY_TO_STRING(orgs, ',') || $$)$$ ||
           $$) TO '$$ ||  dir || $$/permission_grp_penalty_threshold'$$;
   EXECUTE $$COPY (SELECT owning_lib, label, label_sortkey FROM asset.call_number_prefix WHERE owning_lib IN ($$ ||
           ARRAY_TO_STRING(orgs, ',') || $$)$$ ||
           $$) TO '$$ ||  dir || $$/asset_call_number_prefix'$$;
   EXECUTE $$COPY (SELECT owning_lib, label, label_sortkey FROM asset.call_number_suffix WHERE owning_lib IN ($$ ||
           ARRAY_TO_STRING(orgs, ',') || $$)$$ ||
           $$) TO '$$ ||  dir || $$/asset_call_number_suffix'$$;
   EXECUTE $$COPY config.rule_circ_duration TO '$$ ||  dir || $$/config_rule_circ_duration'$$;
   EXECUTE $$COPY config.rule_age_hold_protect TO '$$ ||  dir || $$/config_rule_age_hold_protect'$$;
   EXECUTE $$COPY config.rule_max_fine TO '$$ ||  dir || $$/config_rule_max_fine'$$;
   EXECUTE $$COPY config.rule_recurring_fine TO '$$ ||  dir || $$/config_rule_recurring_fine'$$;
   EXECUTE $$COPY permission.grp_tree TO '$$ ||  dir || $$/permission_grp_tree'$$;
END;
$FUNC$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION migration_tools.simple_import_library_config(dir TEXT) RETURNS VOID AS $FUNC$
BEGIN
   EXECUTE $$COPY actor.hours_of_operation FROM '$$ ||  dir || $$/actor_hours_of_operation'$$;
   EXECUTE $$COPY actor.org_unit_closed (org_unit, close_start, close_end, reason) FROM '$$ ||  dir || $$/actor_org_unit_closed'$$;
   EXECUTE $$COPY actor.org_unit_setting (org_unit, name, value) FROM '$$ ||  dir || $$/actor_org_unit_setting'$$;
   EXECUTE $$COPY asset.copy_location (name, owning_lib, holdable, hold_verify, opac_visible, circulate) FROM '$$ ||  dir || $$/asset_copy_location'$$;
   EXECUTE $$COPY permission.grp_penalty_threshold (grp, org_unit, penalty, threshold) FROM '$$ ||  dir || $$/permission_grp_penalty_threshold'$$;
   EXECUTE $$COPY asset.call_number_prefix (owning_lib, label, label_sortkey) FROM '$$ ||  dir || $$/asset_call_number_prefix'$$;
   EXECUTE $$COPY asset.call_number_suffix (owning_lib, label, label_sortkey) FROM '$$ ||  dir || $$/asset_call_number_suffix'$$;

   -- import any new circ rules
   PERFORM migration_tools.simple_import_new_rows_by_value(dir, 'config', 'rule_circ_duration', 'id', 'name');
   PERFORM migration_tools.simple_import_new_rows_by_value(dir, 'config', 'rule_age_hold_protect', 'id', 'name');
   PERFORM migration_tools.simple_import_new_rows_by_value(dir, 'config', 'rule_max_fine', 'id', 'name');
   PERFORM migration_tools.simple_import_new_rows_by_value(dir, 'config', 'rule_recurring_fine', 'id', 'name');

   -- and permission groups
   PERFORM migration_tools.simple_import_new_rows_by_value(dir, 'permission', 'grp_tree', 'id', 'name');

END;
$FUNC$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION migration_tools.simple_import_new_rows_by_value(dir TEXT, schemaname TEXT, tablename TEXT, idcol TEXT, matchcol TEXT) RETURNS VOID AS $FUNC$
DECLARE
    name TEXT;
    loopq TEXT;
    existsq TEXT;
    ct INTEGER;
    cols TEXT[];
    copyst TEXT;
BEGIN
    EXECUTE $$DROP TABLE IF EXISTS tmp_$$ || tablename;
    EXECUTE $$CREATE TEMPORARY TABLE tmp_$$ || tablename || $$ AS SELECT * FROM $$ || schemaname || '.' || tablename || $$ LIMIT 0$$;
    EXECUTE $$COPY tmp_$$ || tablename || $$ FROM '$$ ||  dir || '/' || schemaname || '_' || tablename || $$'$$;
    loopq := 'SELECT ' || matchcol || ' FROM tmp_' || tablename || ' ORDER BY ' || idcol;
    existsq := 'SELECT COUNT(*) FROM ' || schemaname || '.' || tablename || ' WHERE ' || matchcol || ' = $1';
    SELECT ARRAY_AGG(column_name::TEXT) INTO cols FROM information_schema.columns WHERE table_schema = schemaname AND table_name = tablename AND column_name <> idcol;
    FOR name IN EXECUTE loopq LOOP
       EXECUTE existsq INTO ct USING name;
       IF ct = 0 THEN
           RAISE NOTICE 'inserting %.% row for %', schemaname, tablename, name;
           copyst := 'INSERT INTO ' || schemaname || '.' || tablename || ' (' || ARRAY_TO_STRING(cols, ',') || ') SELECT ' || ARRAY_TO_STRING(cols, ',') || 
                     ' FROM tmp_' || tablename || ' WHERE ' || matchcol || ' = $1';
           EXECUTE copyst USING name;
       END IF;
    END LOOP;
END;
$FUNC$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION migration_tools.split_rows_on_column_with_delimiter(schemaname TEXT, tablename TEXT, matchcol TEXT, delimiter TEXT) RETURNS VOID AS $FUNC$
DECLARE
    id BIGINT;
    loopq TEXT;
    cols TEXT[];
    splitst TEXT;
BEGIN
    loopq := 'SELECT id FROM ' || schemaname || '.' || tablename || ' WHERE ' || matchcol || ' ~ $1 ORDER BY id';
    SELECT ARRAY_AGG(column_name::TEXT) INTO cols FROM information_schema.columns WHERE table_schema = schemaname AND table_name = tablename AND column_name <> 'id' AND column_name <> matchcol;
    FOR id IN EXECUTE loopq USING delimiter LOOP
       RAISE NOTICE 'splitting row from %.% with id = %', schemaname, tablename, id;
       splitst := 'INSERT INTO ' || schemaname || '.' || tablename || ' (' || ARRAY_TO_STRING(cols, ',') || ', ' || matchcol || ') SELECT ' || ARRAY_TO_STRING(cols, ',') || ', s.token ' ||
                 ' FROM ' || schemaname || '.' || tablename || ' t, UNNEST(STRING_TO_ARRAY(t.' || matchcol || ', $2)) s(token) WHERE id = $1';
       EXECUTE splitst USING id, delimiter;
    END LOOP;
END;
$FUNC$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION migration_tools.merge_marc_fields( TEXT, TEXT, TEXT[] ) RETURNS TEXT AS $func$

use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;

MARC::Charset->assume_unicode(1);

my $target_xml = shift;
my $source_xml = shift;
my $tags = shift;

my $target;
my $source;

eval { $target = MARC::Record->new_from_xml( $target_xml ); };
if ($@) {
    return;
}
eval { $source = MARC::Record->new_from_xml( $source_xml ); };
if ($@) {
    return;
}

my $source_id = $source->subfield('901', 'c');
$source_id = $source->subfield('903', 'a') unless $source_id;
my $target_id = $target->subfield('901', 'c');
$target_id = $target->subfield('903', 'a') unless $target_id;

my %existing_fields;
foreach my $tag (@$tags) {
    my %existing_fields = map { $_->as_formatted() => 1 } $target->field($tag);
    my @to_add = grep { not exists $existing_fields{$_->as_formatted()} } $source->field($tag);
    $target->insert_fields_ordered(map { $_->clone() } @to_add);
    if (@to_add) {
        elog(NOTICE, "Merged $tag tag(s) from $source_id to $target_id");
    }
}

my $xml = $target->as_xml_record;
$xml =~ s/^<\?.+?\?>$//mo;
$xml =~ s/\n//sgo;
$xml =~ s/>\s+</></sgo;

return $xml;

$func$ LANGUAGE PLPERLU;
COMMENT ON FUNCTION migration_tools.merge_marc_fields( TEXT, TEXT, TEXT[] ) IS 'Given two MARCXML strings and an array of tags, returns MARCXML representing the merge of the specified fields from the second MARCXML record into the first.';

CREATE OR REPLACE FUNCTION migration_tools.make_stub_bib (text[], text[]) RETURNS TEXT AS $func$

use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use Text::CSV;

my $in_tags = shift;
my $in_values = shift;

# hack-and-slash parsing of array-passed-as-string;
# this can go away once everybody is running Postgres 9.1+
my $csv = Text::CSV->new({binary => 1});
$in_tags =~ s/^{//;
$in_tags =~ s/}$//;
my $status = $csv->parse($in_tags);
my $tags = [ $csv->fields() ];
$in_values =~ s/^{//;
$in_values =~ s/}$//;
$status = $csv->parse($in_values);
my $values = [ $csv->fields() ];

my $marc = MARC::Record->new();

$marc->leader('00000nam a22000007  4500');
$marc->append_fields(MARC::Field->new('008', '000000s                       000   eng d'));

foreach my $i (0..$#$tags) {
    my ($tag, $sf);
    if ($tags->[$i] =~ /^(\d{3})([0-9a-z])$/) {
        $tag = $1;
        $sf = $2;
        $marc->append_fields(MARC::Field->new($tag, ' ', ' ', $sf => $values->[$i])) if $values->[$i] !~ /^\s*$/ and $values->[$i] ne 'NULL';
    } elsif ($tags->[$i] =~ /^(\d{3})$/) {
        $tag = $1;
        $marc->append_fields(MARC::Field->new($tag, $values->[$i])) if $values->[$i] !~ /^\s*$/ and $values->[$i] ne 'NULL';
    }
}

my $xml = $marc->as_xml_record;
$xml =~ s/^<\?.+?\?>$//mo;
$xml =~ s/\n//sgo;
$xml =~ s/>\s+</></sgo;

return $xml;

$func$ LANGUAGE PLPERLU;
COMMENT ON FUNCTION migration_tools.make_stub_bib (text[], text[]) IS $$Simple function to create a stub MARCXML bib from a set of columns.
The first argument is an array of tag/subfield specifiers, e.g., ARRAY['001', '245a', '500a'].
The second argument is an array of text containing the values to plug into each field.  
If the value for a given field is NULL or the empty string, it is not inserted.
$$;

CREATE OR REPLACE FUNCTION migration_tools.set_indicator (TEXT, TEXT, INTEGER, CHAR(1)) RETURNS TEXT AS $func$

my ($marcxml, $tag, $pos, $value) = @_;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use strict;

MARC::Charset->assume_unicode(1);

elog(ERROR, 'indicator position must be either 1 or 2') unless $pos =~ /^[12]$/;
elog(ERROR, 'MARC tag must be numeric') unless $tag =~ /^\d{3}$/;
elog(ERROR, 'MARC tag must not be control field') if $tag =~ /^00/;
elog(ERROR, 'Value must be exactly one character') unless $value =~ /^.$/;

my $xml = $marcxml;
eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');

    foreach my $field ($marc->field($tag)) {
        $field->update("ind$pos" => $value);
    }
    $xml = $marc->as_xml_record;
    $xml =~ s/^<\?.+?\?>$//mo;
    $xml =~ s/\n//sgo;
    $xml =~ s/>\s+</></sgo;
};
return $xml;

$func$ LANGUAGE PLPERLU;

COMMENT ON FUNCTION migration_tools.set_indicator(TEXT, TEXT, INTEGER, CHAR(1)) IS $$Set indicator value of a specified MARC field.
The first argument is a MARCXML string.
The second argument is a MARC tag.
The third argument is the indicator position, either 1 or 2.
The fourth argument is the character to set the indicator value to.
All occurences of the specified field will be changed.
The function returns the revised MARCXML string.$$;

CREATE OR REPLACE FUNCTION migration_tools.create_staff_user(
    username TEXT,
    password TEXT,
    org TEXT,
    perm_group TEXT,
    first_name TEXT DEFAULT '',
    last_name TEXT DEFAULT ''
) RETURNS VOID AS $func$
BEGIN
    RAISE NOTICE '%', org ;
    INSERT INTO actor.usr (usrname, passwd, ident_type, first_given_name, family_name, home_ou, profile)
    SELECT username, password, 1, first_name, last_name, aou.id, pgt.id
    FROM   actor.org_unit aou, permission.grp_tree pgt
    WHERE  aou.shortname = org
    AND    pgt.name = perm_group;
END
$func$
LANGUAGE PLPGSQL;

-- example: SELECT * FROM migration_tools.duplicate_template(5,'{3,4}');
CREATE OR REPLACE FUNCTION migration_tools.duplicate_template (INTEGER, INTEGER[]) RETURNS VOID AS $$
    DECLARE
        target_event_def ALIAS FOR $1;
        orgs ALIAS FOR $2;
    BEGIN
        DROP TABLE IF EXISTS new_atevdefs;
        CREATE TEMP TABLE new_atevdefs (atevdef INTEGER);
        FOR i IN array_lower(orgs,1) .. array_upper(orgs,1) LOOP
            INSERT INTO action_trigger.event_definition (
                active
                ,owner
                ,name
                ,hook
                ,validator
                ,reactor
                ,cleanup_success
                ,cleanup_failure
                ,delay
                ,max_delay
                ,usr_field
                ,opt_in_setting
                ,delay_field
                ,group_field
                ,template
                ,granularity
                ,repeat_delay
            ) SELECT
                'f'
                ,orgs[i]
                ,name || ' (clone of '||target_event_def||')'
                ,hook
                ,validator
                ,reactor
                ,cleanup_success
                ,cleanup_failure
                ,delay
                ,max_delay
                ,usr_field
                ,opt_in_setting
                ,delay_field
                ,group_field
                ,template
                ,granularity
                ,repeat_delay
            FROM
                action_trigger.event_definition
            WHERE
                id = target_event_def
            ;
            RAISE INFO 'created atevdef with id = %', currval('action_trigger.event_definition_id_seq');
            INSERT INTO new_atevdefs SELECT currval('action_trigger.event_definition_id_seq');
            INSERT INTO action_trigger.environment (
                event_def
                ,path
                ,collector
                ,label
            ) SELECT
                currval('action_trigger.event_definition_id_seq')
                ,path
                ,collector
                ,label
            FROM
                action_trigger.environment
            WHERE
                event_def = target_event_def
            ;
            INSERT INTO action_trigger.event_params (
                event_def
                ,param
                ,value
            ) SELECT
                currval('action_trigger.event_definition_id_seq')
                ,param
                ,value
            FROM
                action_trigger.event_params
            WHERE
                event_def = target_event_def
            ;
        END LOOP;
        RAISE INFO '-- UPDATE action_trigger.event_definition SET active = CASE WHEN id = % THEN FALSE ELSE TRUE END WHERE id in (%,%);', target_event_def, target_event_def, (SELECT array_to_string(array_agg(atevdef),',') from new_atevdefs);
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- example: SELECT * FROM migration_tools.duplicate_template_but_change_delay(5,'{3,4}','00:30:00'::INTERVAL);
CREATE OR REPLACE FUNCTION migration_tools.duplicate_template_but_change_delay (INTEGER, INTEGER[], INTERVAL) RETURNS VOID AS $$
    DECLARE
        target_event_def ALIAS FOR $1;
        orgs ALIAS FOR $2;
        new_interval ALIAS FOR $3;
    BEGIN
        DROP TABLE IF EXISTS new_atevdefs;
        CREATE TEMP TABLE new_atevdefs (atevdef INTEGER);
        FOR i IN array_lower(orgs,1) .. array_upper(orgs,1) LOOP
            INSERT INTO action_trigger.event_definition (
                active
                ,owner
                ,name
                ,hook
                ,validator
                ,reactor
                ,cleanup_success
                ,cleanup_failure
                ,delay
                ,max_delay
                ,usr_field
                ,opt_in_setting
                ,delay_field
                ,group_field
                ,template
                ,granularity
                ,repeat_delay
            ) SELECT
                'f'
                ,orgs[i]
                ,name || ' (clone of '||target_event_def||')'
                ,hook
                ,validator
                ,reactor
                ,cleanup_success
                ,cleanup_failure
                ,new_interval
                ,max_delay
                ,usr_field
                ,opt_in_setting
                ,delay_field
                ,group_field
                ,template
                ,granularity
                ,repeat_delay
            FROM
                action_trigger.event_definition
            WHERE
                id = target_event_def
            ;
            RAISE INFO 'created atevdef with id = %', currval('action_trigger.event_definition_id_seq');
            INSERT INTO new_atevdefs SELECT currval('action_trigger.event_definition_id_seq');
            INSERT INTO action_trigger.environment (
                event_def
                ,path
                ,collector
                ,label
            ) SELECT
                currval('action_trigger.event_definition_id_seq')
                ,path
                ,collector
                ,label
            FROM
                action_trigger.environment
            WHERE
                event_def = target_event_def
            ;
            INSERT INTO action_trigger.event_params (
                event_def
                ,param
                ,value
            ) SELECT
                currval('action_trigger.event_definition_id_seq')
                ,param
                ,value
            FROM
                action_trigger.event_params
            WHERE
                event_def = target_event_def
            ;
        END LOOP;
        RAISE INFO '-- UPDATE action_trigger.event_definition SET active = CASE WHEN id = % THEN FALSE ELSE TRUE END WHERE id in (%,%);', target_event_def, target_event_def, (SELECT array_to_string(array_agg(atevdef),',') from new_atevdefs);
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- example: SELECT * FROM migration_tools.duplicate_templates(3,'{5,6}');
CREATE OR REPLACE FUNCTION migration_tools.duplicate_templates (INTEGER, INTEGER[]) RETURNS VOID AS $$
    DECLARE
        org ALIAS FOR $1;
        target_event_defs ALIAS FOR $2;
    BEGIN
        DROP TABLE IF EXISTS new_atevdefs;
        CREATE TEMP TABLE new_atevdefs (atevdef INTEGER);
        FOR i IN array_lower(target_event_defs,1) .. array_upper(target_event_defs,1) LOOP
            INSERT INTO action_trigger.event_definition (
                active
                ,owner
                ,name
                ,hook
                ,validator
                ,reactor
                ,cleanup_success
                ,cleanup_failure
                ,delay
                ,max_delay
                ,usr_field
                ,opt_in_setting
                ,delay_field
                ,group_field
                ,template
                ,granularity
                ,repeat_delay
            ) SELECT
                'f'
                ,org
                ,name || ' (clone of '||target_event_defs[i]||')'
                ,hook
                ,validator
                ,reactor
                ,cleanup_success
                ,cleanup_failure
                ,delay
                ,max_delay
                ,usr_field
                ,opt_in_setting
                ,delay_field
                ,group_field
                ,template
                ,granularity
                ,repeat_delay
            FROM
                action_trigger.event_definition
            WHERE
                id = target_event_defs[i]
            ;
            RAISE INFO 'created atevdef with id = %', currval('action_trigger.event_definition_id_seq');
            INSERT INTO new_atevdefs SELECT currval('action_trigger.event_definition_id_seq');
            INSERT INTO action_trigger.environment (
                event_def
                ,path
                ,collector
                ,label
            ) SELECT
                currval('action_trigger.event_definition_id_seq')
                ,path
                ,collector
                ,label
            FROM
                action_trigger.environment
            WHERE
                event_def = target_event_defs[i]
            ;
            INSERT INTO action_trigger.event_params (
                event_def
                ,param
                ,value
            ) SELECT
                currval('action_trigger.event_definition_id_seq')
                ,param
                ,value
            FROM
                action_trigger.event_params
            WHERE
                event_def = target_event_defs[i]
            ;
        END LOOP;
        RAISE INFO '-- UPDATE action_trigger.event_definition SET active = TRUE WHERE id in (%);', (SELECT array_to_string(array_agg(atevdef),',') from new_atevdefs);
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.reset_event (BIGINT) RETURNS VOID AS $$
    UPDATE
        action_trigger.event
    SET
         start_time = NULL
        ,update_time = NULL
        ,complete_time = NULL
        ,update_process = NULL
        ,state = 'pending'
        ,template_output = NULL
        ,error_output = NULL
        ,async_output = NULL
    WHERE
        id = $1;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION migration_tools.get_marc_leader (TEXT) RETURNS TEXT AS $$
    my ($marcxml) = @_;

    use MARC::Record;
    use MARC::File::XML;
    use MARC::Field;

    my $field;
    eval {
        my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
        $field = $marc->leader();
    };
    return $field;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.get_marc_tag (TEXT, TEXT, TEXT, TEXT) RETURNS TEXT AS $$
    my ($marcxml, $tag, $subfield, $delimiter) = @_;

    use MARC::Record;
    use MARC::File::XML;
    use MARC::Field;

    my $field;
    eval {
        my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
        $field = $marc->field($tag);
    };
    return $field->as_string($subfield,$delimiter);
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.get_marc_tags (TEXT, TEXT, TEXT, TEXT) RETURNS TEXT[] AS $$
    my ($marcxml, $tag, $subfield, $delimiter) = @_;

    use MARC::Record;
    use MARC::File::XML;
    use MARC::Field;

    my @fields;
    eval {
        my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
        @fields = $marc->field($tag);
    };
    my @texts;
    foreach my $field (@fields) {
        push @texts, $field->as_string($subfield,$delimiter);
    }
    return \@texts;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.get_marc_tags_filtered (TEXT, TEXT, TEXT, TEXT, TEXT) RETURNS TEXT[] AS $$
    my ($marcxml, $tag, $subfield, $delimiter, $match) = @_;

    use MARC::Record;
    use MARC::File::XML;
    use MARC::Field;

    my @fields;
    eval {
        my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
        @fields = $marc->field($tag);
    };
    my @texts;
    foreach my $field (@fields) {
        if ($field->as_string() =~ qr/$match/) {
            push @texts, $field->as_string($subfield,$delimiter);
        }
    }
    return \@texts;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.find_hold_matrix_matchpoint (INTEGER) RETURNS INTEGER AS $$
    SELECT action.find_hold_matrix_matchpoint(
        (SELECT pickup_lib FROM action.hold_request WHERE id = $1),
        (SELECT request_lib FROM action.hold_request WHERE id = $1),
        (SELECT current_copy FROM action.hold_request WHERE id = $1),
        (SELECT usr FROM action.hold_request WHERE id = $1),
        (SELECT requestor FROM action.hold_request WHERE id = $1)
    );
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION migration_tools.find_hold_matrix_matchpoint2 (INTEGER) RETURNS SETOF action.matrix_test_result AS $$
    SELECT action.hold_request_permit_test(
        (SELECT pickup_lib FROM action.hold_request WHERE id = $1),
        (SELECT request_lib FROM action.hold_request WHERE id = $1),
        (SELECT current_copy FROM action.hold_request WHERE id = $1),
        (SELECT usr FROM action.hold_request WHERE id = $1),
        (SELECT requestor FROM action.hold_request WHERE id = $1)
    );
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION migration_tools.find_circ_matrix_matchpoint (INTEGER) RETURNS SETOF action.found_circ_matrix_matchpoint AS $$
    SELECT action.find_circ_matrix_matchpoint(
        (SELECT circ_lib FROM action.circulation WHERE id = $1),
        (SELECT target_copy FROM action.circulation WHERE id = $1),
        (SELECT usr FROM action.circulation WHERE id = $1),
        (SELECT COALESCE(
                NULLIF(phone_renewal,false),
                NULLIF(desk_renewal,false),
                NULLIF(opac_renewal,false),
                false
            ) FROM action.circulation WHERE id = $1
        )
    );
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION migration_tools.assert (BOOLEAN) RETURNS VOID AS $$
    DECLARE
        test ALIAS FOR $1;
    BEGIN
        IF NOT test THEN
            RAISE EXCEPTION 'assertion';
        END IF;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.assert (BOOLEAN,TEXT) RETURNS VOID AS $$
    DECLARE
        test ALIAS FOR $1;
        msg ALIAS FOR $2;
    BEGIN
        IF NOT test THEN
            RAISE EXCEPTION '%', msg;
        END IF;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.assert (BOOLEAN,TEXT,TEXT) RETURNS TEXT AS $$
    DECLARE
        test ALIAS FOR $1;
        fail_msg ALIAS FOR $2;
        success_msg ALIAS FOR $3;
    BEGIN
        IF NOT test THEN
            RAISE EXCEPTION '%', fail_msg;
        END IF;
        RETURN success_msg;
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

-- set a new salted password

CREATE OR REPLACE FUNCTION migration_tools.set_salted_passwd(INTEGER,TEXT) RETURNS BOOLEAN AS $$
    DECLARE
        usr_id              ALIAS FOR $1;
        plain_passwd        ALIAS FOR $2;
        plain_salt          TEXT;
        md5_passwd          TEXT;
    BEGIN

        SELECT actor.create_salt('main') INTO plain_salt;

        SELECT MD5(plain_passwd) INTO md5_passwd;
        
        PERFORM actor.set_passwd(usr_id, 'main', MD5(plain_salt || md5_passwd), plain_salt);

        RETURN TRUE;

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;


-- convenience functions for handling copy_location maps
CREATE OR REPLACE FUNCTION migration_tools.handle_shelf (TEXT,TEXT,TEXT,INTEGER) RETURNS VOID AS $$
    SELECT migration_tools._handle_shelf($1,$2,$3,$4,TRUE);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION migration_tools._handle_shelf (TEXT,TEXT,TEXT,INTEGER,BOOLEAN) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        org_shortname ALIAS FOR $3;
        org_range ALIAS FOR $4;
        make_assertion ALIAS FOR $5;
        proceed BOOLEAN;
        org INTEGER;
        -- if x_org is on the mapping table, it'll take precedence over the passed org_shortname param
        -- though we'll still use the passed org for the full path traversal when needed
        x_org_found BOOLEAN;
        x_org INTEGER;
        org_list INTEGER[];
        o INTEGER;
        row_count NUMERIC;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''desired_shelf''
        )' INTO proceed USING table_schema, table_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column desired_shelf';
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''x_org''
        )' INTO x_org_found USING table_schema, table_name;

        SELECT id INTO org FROM actor.org_unit WHERE shortname = org_shortname;
        IF org IS NULL THEN
            RAISE EXCEPTION 'Cannot find org by shortname';
        END IF;

        SELECT INTO org_list ARRAY_ACCUM(id) FROM actor.org_unit_full_path( org );

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_shelf';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_shelf INTEGER';

        IF x_org_found THEN
            RAISE INFO 'Found x_org column';
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_shelf = b.id FROM asset_copy_location b'
                || ' WHERE BTRIM(UPPER(a.desired_shelf)) = BTRIM(UPPER(b.name))'
                || ' AND b.owning_lib = x_org'
                || ' AND NOT b.deleted';
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_shelf = b.id FROM asset.copy_location b'
                || ' WHERE BTRIM(UPPER(a.desired_shelf)) = BTRIM(UPPER(b.name))'
                || ' AND b.owning_lib = x_org'
                || ' AND x_shelf IS NULL'
                || ' AND NOT b.deleted';
        ELSE
            RAISE INFO 'Did not find x_org column';
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_shelf = b.id FROM asset_copy_location b'
                || ' WHERE BTRIM(UPPER(a.desired_shelf)) = BTRIM(UPPER(b.name))'
                || ' AND b.owning_lib = $1'
                || ' AND NOT b.deleted'
            USING org;
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_shelf = b.id FROM asset_copy_location b'
                || ' WHERE BTRIM(UPPER(a.desired_shelf)) = BTRIM(UPPER(b.name))'
                || ' AND b.owning_lib = $1'
                || ' AND x_shelf IS NULL'
                || ' AND NOT b.deleted'
            USING org;
        END IF;

        FOREACH o IN ARRAY org_list LOOP
            RAISE INFO 'Considering org %', o;
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_shelf = b.id FROM asset.copy_location b'
                || ' WHERE BTRIM(UPPER(a.desired_shelf)) = BTRIM(UPPER(b.name))'
                || ' AND b.owning_lib = $1 AND x_shelf IS NULL'
                || ' AND NOT b.deleted'
            USING o;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            RAISE INFO 'Updated % rows', row_count;
        END LOOP;

        IF make_assertion THEN
            EXECUTE 'SELECT migration_tools.assert(
                NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_shelf <> '''' AND x_shelf IS NULL),
                ''Cannot find a desired location'',
                ''Found all desired locations''
            );';
        END IF;

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience functions for handling circmod maps

CREATE OR REPLACE FUNCTION migration_tools.handle_circmod (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''desired_circmod''
        )' INTO proceed USING table_schema, table_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column desired_circmod'; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_circmod';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_circmod TEXT';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_circmod = code FROM config.circ_modifier b'
            || ' WHERE BTRIM(UPPER(a.desired_circmod)) = BTRIM(UPPER(b.code))';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_circmod = code FROM config.circ_modifier b'
            || ' WHERE BTRIM(UPPER(a.desired_circmod)) = BTRIM(UPPER(b.name))'
            || ' AND x_circmod IS NULL';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_circmod = code FROM config.circ_modifier b'
            || ' WHERE BTRIM(UPPER(a.desired_circmod)) = BTRIM(UPPER(b.description))'
            || ' AND x_circmod IS NULL';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_circmod <> '''' AND x_circmod IS NULL),
            ''Cannot find a desired circulation modifier'',
            ''Found all desired circulation modifiers''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience functions for handling item status maps

CREATE OR REPLACE FUNCTION migration_tools.handle_status (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''desired_status''
        )' INTO proceed USING table_schema, table_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column desired_status'; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_status';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_status INTEGER';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_status = id FROM config.copy_status b'
            || ' WHERE BTRIM(UPPER(a.desired_status)) = BTRIM(UPPER(b.name))';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_status <> '''' AND x_status IS NULL),
            ''Cannot find a desired copy status'',
            ''Found all desired copy statuses''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience functions for handling org maps

CREATE OR REPLACE FUNCTION migration_tools.handle_org (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''desired_org''
        )' INTO proceed USING table_schema, table_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column desired_org'; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_org';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_org INTEGER';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_org = b.id FROM actor.org_unit b'
            || ' WHERE BTRIM(a.desired_org) = BTRIM(b.shortname)';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_org <> '''' AND x_org IS NULL),
            ''Cannot find a desired org unit'',
            ''Found all desired org units''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for handling desired_not_migrate

CREATE OR REPLACE FUNCTION migration_tools.handle_not_migrate (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''desired_not_migrate''
        )' INTO proceed USING table_schema, table_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column desired_not_migrate'; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_migrate';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_migrate BOOLEAN';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_migrate = CASE'
            || ' WHEN BTRIM(desired_not_migrate) = ''TRUE'' THEN FALSE'
            || ' WHEN BTRIM(desired_not_migrate) = ''DNM'' THEN FALSE'
            || ' WHEN BTRIM(desired_not_migrate) = ''Do Not Migrate'' THEN FALSE'
            || ' WHEN BTRIM(desired_not_migrate) = ''FALSE'' THEN TRUE'
            || ' WHEN BTRIM(desired_not_migrate) = ''Migrate'' THEN TRUE'
            || ' WHEN BTRIM(desired_not_migrate) = '''' THEN TRUE'
            || ' END';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE x_migrate IS NULL),
            ''Not all desired_not_migrate values understood'',
            ''All desired_not_migrate values understood''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for handling desired_not_migrate

CREATE OR REPLACE FUNCTION migration_tools.handle_barred_or_blocked (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''desired_barred_or_blocked''
        )' INTO proceed USING table_schema, table_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column desired_barred_or_blocked'; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_barred';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_barred BOOLEAN';

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_blocked';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_blocked BOOLEAN';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_barred = CASE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = ''Barred'' THEN TRUE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = ''Blocked'' THEN FALSE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = ''Neither'' THEN FALSE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = '''' THEN FALSE'
            || ' END';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_blocked = CASE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = ''Blocked'' THEN TRUE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = ''Barred'' THEN FALSE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = ''Neither'' THEN FALSE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = '''' THEN FALSE'
            || ' END';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE x_barred IS NULL or x_blocked IS NULL),
            ''Not all desired_barred_or_blocked values understood'',
            ''All desired_barred_or_blocked values understood''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for handling desired_profile

CREATE OR REPLACE FUNCTION migration_tools.handle_profile (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''desired_profile''
        )' INTO proceed USING table_schema, table_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column desired_profile'; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_profile';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_profile INTEGER';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_profile = b.id FROM permission.grp_tree b'
            || ' WHERE BTRIM(UPPER(a.desired_profile)) = BTRIM(UPPER(b.name))';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_profile <> '''' AND x_profile IS NULL),
            ''Cannot find a desired profile'',
            ''Found all desired profiles''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for handling desired actor stat cats

CREATE OR REPLACE FUNCTION migration_tools.vivicate_actor_sc_and_sce (TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        field_suffix ALIAS FOR $3; -- for distinguishing between desired_sce1, desired_sce2, etc.
        org_shortname ALIAS FOR $4;
        proceed BOOLEAN;
        org INTEGER;
        org_list INTEGER[];
        sc TEXT;
        sce TEXT;
    BEGIN

        SELECT 'desired_sc' || field_suffix INTO sc;
        SELECT 'desired_sce' || field_suffix INTO sce;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sc;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sc; 
        END IF;
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sce;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sce; 
        END IF;

        SELECT id INTO org FROM actor.org_unit WHERE shortname = org_shortname;
        IF org IS NULL THEN
            RAISE EXCEPTION 'Cannot find org by shortname';
        END IF;
        SELECT INTO org_list ARRAY_ACCUM(id) FROM actor.org_unit_full_path( org );

        -- caller responsible for their own truncates though we try to prevent duplicates
        EXECUTE 'INSERT INTO actor_stat_cat (owner, name)
            SELECT DISTINCT
                 $1
                ,BTRIM('||sc||')
            FROM 
                ' || quote_ident(table_name) || '
            WHERE
                NULLIF(BTRIM('||sc||'),'''') IS NOT NULL
                AND NOT EXISTS (
                    SELECT id
                    FROM actor.stat_cat
                    WHERE owner = ANY ($2)
                    AND name = BTRIM('||sc||')
                )
                AND NOT EXISTS (
                    SELECT id
                    FROM actor_stat_cat
                    WHERE owner = ANY ($2)
                    AND name = BTRIM('||sc||')
                )
            ORDER BY 2;'
        USING org, org_list;

        EXECUTE 'INSERT INTO actor_stat_cat_entry (stat_cat, owner, value)
            SELECT DISTINCT
                COALESCE(
                    (SELECT id
                        FROM actor.stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name))
                   ,(SELECT id
                        FROM actor_stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name))
                )
                ,$1
                ,BTRIM('||sce||')
            FROM 
                ' || quote_ident(table_name) || '
            WHERE
                    NULLIF(BTRIM('||sc||'),'''') IS NOT NULL
                AND NULLIF(BTRIM('||sce||'),'''') IS NOT NULL
                AND NOT EXISTS (
                    SELECT id
                    FROM actor.stat_cat_entry
                    WHERE stat_cat = (
                        SELECT id
                        FROM actor.stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name)
                    ) AND value = BTRIM('||sce||')
                    AND owner = ANY ($2)
                )
                AND NOT EXISTS (
                    SELECT id
                    FROM actor_stat_cat_entry
                    WHERE stat_cat = (
                        SELECT id
                        FROM actor_stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name)
                    ) AND value = BTRIM('||sce||')
                    AND owner = ANY ($2)
                )
            ORDER BY 1,3;'
        USING org, org_list;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.handle_actor_sc_and_sce (TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        field_suffix ALIAS FOR $3; -- for distinguishing between desired_sce1, desired_sce2, etc.
        org_shortname ALIAS FOR $4;
        proceed BOOLEAN;
        org INTEGER;
        org_list INTEGER[];
        o INTEGER;
        sc TEXT;
        sce TEXT;
    BEGIN
        SELECT 'desired_sc' || field_suffix INTO sc;
        SELECT 'desired_sce' || field_suffix INTO sce;
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sc;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sc; 
        END IF;
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sce;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sce; 
        END IF;

        SELECT id INTO org FROM actor.org_unit WHERE shortname = org_shortname;
        IF org IS NULL THEN
            RAISE EXCEPTION 'Cannot find org by shortname';
        END IF;

        SELECT INTO org_list ARRAY_ACCUM(id) FROM actor.org_unit_full_path( org );

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_sc' || field_suffix;
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_sc' || field_suffix || ' INTEGER';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_sce' || field_suffix;
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_sce' || field_suffix || ' INTEGER';


        EXECUTE 'UPDATE ' || quote_ident(table_name) || '
            SET
                x_sc' || field_suffix || ' = id
            FROM
                (SELECT id, name, owner FROM actor_stat_cat
                    UNION SELECT id, name, owner FROM actor.stat_cat) u
            WHERE
                    BTRIM(UPPER(u.name)) = BTRIM(UPPER(' || sc || '))
                AND u.owner = ANY ($1);'
        USING org_list;

        EXECUTE 'UPDATE ' || quote_ident(table_name) || '
            SET
                x_sce' || field_suffix || ' = id
            FROM
                (SELECT id, stat_cat, owner, value FROM actor_stat_cat_entry
                    UNION SELECT id, stat_cat, owner, value FROM actor.stat_cat_entry) u
            WHERE
                    u.stat_cat = x_sc' || field_suffix || '
                AND BTRIM(UPPER(u.value)) = BTRIM(UPPER(' || sce || '))
                AND u.owner = ANY ($1);'
        USING org_list;

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_sc' || field_suffix || ' <> '''' AND x_sc' || field_suffix || ' IS NULL),
            ''Cannot find a desired stat cat'',
            ''Found all desired stat cats''
        );';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_sce' || field_suffix || ' <> '''' AND x_sce' || field_suffix || ' IS NULL),
            ''Cannot find a desired stat cat entry'',
            ''Found all desired stat cat entries''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience functions for adding shelving locations
DROP FUNCTION IF EXISTS migration_tools.find_shelf(INT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.find_shelf(org_id INT, shelf_name TEXT) RETURNS INTEGER AS $$
DECLARE
    return_id   INT;
    d           INT;
    cur_id      INT;
BEGIN
    SELECT INTO d MAX(distance) FROM actor.org_unit_ancestors_distance(org_id);
    WHILE d >= 0
    LOOP
        SELECT INTO cur_id id FROM actor.org_unit_ancestor_at_depth(org_id,d);
        SELECT INTO return_id id FROM asset.copy_location WHERE owning_lib = cur_id AND name ILIKE shelf_name;
        IF return_id IS NOT NULL THEN
                RETURN return_id;
        END IF;
        d := d - 1;
    END LOOP;

    RETURN NULL;
END
$$ LANGUAGE plpgsql;

-- may remove later but testing using this with new migration scripts and not loading acls until go live

DROP FUNCTION IF EXISTS migration_tools.find_mig_shelf(INT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.find_mig_shelf(org_id INT, shelf_name TEXT) RETURNS INTEGER AS $$
DECLARE
    return_id   INT;
    d           INT;
    cur_id      INT;
BEGIN
    SELECT INTO d MAX(distance) FROM actor.org_unit_ancestors_distance(org_id);
    WHILE d >= 0
    LOOP
        SELECT INTO cur_id id FROM actor.org_unit_ancestor_at_depth(org_id,d);
        
        SELECT INTO return_id id FROM 
            (SELECT * FROM asset.copy_location UNION ALL SELECT * FROM asset_copy_location) x
            WHERE owning_lib = cur_id AND name ILIKE shelf_name;
        IF return_id IS NOT NULL THEN
                RETURN return_id;
        END IF;
        d := d - 1;
    END LOOP;

    RETURN NULL;
END
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS migration_tools.munge_sf9(INTEGER,TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.merge_group(bib_id INTEGER,new_sf9 TEXT,force TEXT DEFAULT 'false')
    RETURNS BOOLEAN AS 
$BODY$
DECLARE
	marc_xml	TEXT;
	new_marc	TEXT;
BEGIN
	SELECT marc FROM biblio.record_entry WHERE id = bib_id INTO marc_xml;
	
	SELECT munge_sf9(marc_xml,new_sf9,force) INTO new_marc;
	UPDATE biblio.record_entry SET marc = new_marc WHERE id = bib_id;
	
	RETURN true;
END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS migration_tools.munge_sf9(TEXT,TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.munge_sf9(marc_xml TEXT, new_9_to_set TEXT, force TEXT)
 RETURNS TEXT
 LANGUAGE plperlu
AS $function$
use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $marc_xml = shift;
my $new_9_to_set = shift;
my $force = shift;

$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my @uris = $marc_xml->field('856');
return $marc_xml->as_xml_record() unless @uris;

foreach my $field (@uris) {
    my $ind1 = $field->indicator('1');
    if (!defined $ind1) { next; }
    if ($ind1 ne '1' && $ind1 ne '4' && $force eq 'false') { next; }
	if ($ind1 ne '1' && $ind1 ne '4' && $force eq 'true') { $field->set_indicator(1,'4'); }
    my $ind2 = $field->indicator('2');
    if (!defined $ind2) { next; }
    if ($ind2 ne '0' && $ind2 ne '1' && $force eq 'false') { next; }
    if ($ind2 ne '0' && $ind2 ne '1' && $force eq 'true') { $field->set_indicator(2,'0'); }
    $field->add_subfields( '9' => $new_9_to_set );
}

return $marc_xml->as_xml_record();

$function$;

DROP FUNCTION IF EXISTS migration_tools.munge_sf9_qualifying_match(TEXT,TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.munge_sf9_qualifying_match(marc_xml TEXT, qualifying_match TEXT, new_9_to_set TEXT, force TEXT)
 RETURNS TEXT
 LANGUAGE plperlu
AS $function$
use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $marc_xml = shift;
my $qualifying_match = shift;
my $new_9_to_set = shift;
my $force = shift;

$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my @uris = $marc_xml->field('856');
return $marc_xml->as_xml_record() unless @uris;

foreach my $field (@uris) {
    if ($field->as_string() =~ qr/$qualifying_match/) {
        my $ind1 = $field->indicator('1');
        if (!defined $ind1) { next; }
        if ($ind1 ne '1' && $ind1 ne '4' && $force eq 'false') { next; }
        if ($ind1 ne '1' && $ind1 ne '4' && $force eq 'true') { $field->set_indicator(1,'4'); }
        my $ind2 = $field->indicator('2');
        if (!defined $ind2) { next; }
        if ($ind2 ne '0' && $ind2 ne '1' && $force eq 'false') { next; }
        if ($ind2 ne '0' && $ind2 ne '1' && $force eq 'true') { $field->set_indicator(2,'0'); }
        $field->add_subfields( '9' => $new_9_to_set );
    }
}

return $marc_xml->as_xml_record();

$function$;

DROP FUNCTION IF EXISTS migration_tools.owner_change_sf9_substring_match(TEXT,TEXT,TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.owner_change_sf9_substring_match (marc_xml TEXT, substring_old_value TEXT, new_value TEXT, fix_indicators TEXT)
 RETURNS TEXT
 LANGUAGE plperlu
AS $function$
use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $marc_xml = shift;
my $substring_old_value = shift;
my $new_value = shift;
my $fix_indicators = shift;

$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my @uris = $marc_xml->field('856');
return $marc_xml->as_xml_record() unless @uris;

foreach my $field (@uris) {
    my $ind1 = $field->indicator('1');
    if (defined $ind1) {
	    if ($ind1 ne '1' && $ind1 ne '4' && $fix_indicators eq 'true') {
            $field->set_indicator(1,'4');
        }
    }
    my $ind2 = $field->indicator('2');
    if (defined $ind2) {
        if ($ind2 ne '0' && $ind2 ne '1' && $fix_indicators eq 'true') {
            $field->set_indicator(2,'0');
        }
    }
    if ($field->as_string('9') =~ qr/$substring_old_value/) {
        $field->delete_subfield('9');
        $field->add_subfields( '9' => $new_value );
    }
    $marc_xml->delete_field($field); # -- we're going to dedup and add them back
}

my %hash = (map { ($_->as_usmarc => $_) } @uris); # -- courtesy of an old Mike Rylander post :-)
$marc_xml->insert_fields_ordered( values( %hash ) );

return $marc_xml->as_xml_record();

$function$;

DROP FUNCTION IF EXISTS migration_tools.owner_change_sf9_substring_match2(TEXT,TEXT,TEXT,TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.owner_change_sf9_substring_match2 (marc_xml TEXT, qualifying_match TEXT, substring_old_value TEXT, new_value TEXT, fix_indicators TEXT)
 RETURNS TEXT
 LANGUAGE plperlu
AS $function$
use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $marc_xml = shift;
my $qualifying_match = shift;
my $substring_old_value = shift;
my $new_value = shift;
my $fix_indicators = shift;

$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my @unqualified_uris = $marc_xml->field('856');
my @uris = ();
foreach my $field (@unqualified_uris) {
    if ($field->as_string() =~ qr/$qualifying_match/) {
        push @uris, $field;
    }
}
return $marc_xml->as_xml_record() unless @uris;

foreach my $field (@uris) {
    my $ind1 = $field->indicator('1');
    if (defined $ind1) {
	    if ($ind1 ne '1' && $ind1 ne '4' && $fix_indicators eq 'true') {
            $field->set_indicator(1,'4');
        }
    }
    my $ind2 = $field->indicator('2');
    if (defined $ind2) {
        if ($ind2 ne '0' && $ind2 ne '1' && $fix_indicators eq 'true') {
            $field->set_indicator(2,'0');
        }
    }
    if ($field->as_string('9') =~ qr/$substring_old_value/) {
        $field->delete_subfield('9');
        $field->add_subfields( '9' => $new_value );
    }
    $marc_xml->delete_field($field); # -- we're going to dedup and add them back
}

my %hash = (map { ($_->as_usmarc => $_) } @uris); # -- courtesy of an old Mike Rylander post :-)
$marc_xml->insert_fields_ordered( values( %hash ) );

return $marc_xml->as_xml_record();

$function$;

-- strip marc tag
DROP FUNCTION IF EXISTS migration_tools.strip_tag(TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.strip_tag(marc TEXT, tag TEXT)
 RETURNS TEXT
 LANGUAGE plperlu
AS $function$
use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $marc_xml = shift;
my $tag = shift;

$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my @fields = $marc_xml->field($tag);
return $marc_xml->as_xml_record() unless @fields;

$marc_xml->delete_fields(@fields);

return $marc_xml->as_xml_record();

$function$;

-- convenience function for linking to the item staging table

CREATE OR REPLACE FUNCTION migration_tools.handle_item_barcode (TEXT,TEXT,TEXT,TEXT,BOOLEAN) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        foreign_column_name ALIAS FOR $3;
        main_column_name ALIAS FOR $4;
        btrim_desired ALIAS FOR $5;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, foreign_column_name;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_name, foreign_column_name; 
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = ''asset_copy_legacy''
            and column_name = $2
        )' INTO proceed USING table_schema, main_column_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'No %.asset_copy_legacy with column %', table_schema, main_column_name; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_item';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_item BIGINT';

        IF btrim_desired THEN
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_item = b.id FROM asset_copy_legacy b'
                || ' WHERE BTRIM(a.' || quote_ident(foreign_column_name)
                || ') = BTRIM(b.' || quote_ident(main_column_name) || ')';
        ELSE
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_item = b.id FROM asset_copy_legacy b'
                || ' WHERE a.' || quote_ident(foreign_column_name)
                || ' = b.' || quote_ident(main_column_name);
        END IF;

        --EXECUTE 'SELECT migration_tools.assert(
        --    NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE ' || quote_ident(foreign_column_name) || ' <> '''' AND x_item IS NULL),
        --    ''Cannot link every barcode'',
        --    ''Every barcode linked''
        --);';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for linking to the user staging table

CREATE OR REPLACE FUNCTION migration_tools.handle_user_barcode (TEXT,TEXT,TEXT,TEXT,BOOLEAN) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        foreign_column_name ALIAS FOR $3;
        main_column_name ALIAS FOR $4;
        btrim_desired ALIAS FOR $5;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, foreign_column_name;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_name, foreign_column_name; 
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = ''actor_usr_legacy''
            and column_name = $2
        )' INTO proceed USING table_schema, main_column_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'No %.actor_usr_legacy with column %', table_schema, main_column_name; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_user';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_user INTEGER';

        IF btrim_desired THEN
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_user = b.id FROM actor_usr_legacy b'
                || ' WHERE BTRIM(a.' || quote_ident(foreign_column_name)
                || ') = BTRIM(b.' || quote_ident(main_column_name) || ')';
        ELSE
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_user = b.id FROM actor_usr_legacy b'
                || ' WHERE a.' || quote_ident(foreign_column_name)
                || ' = b.' || quote_ident(main_column_name);
        END IF;

        --EXECUTE 'SELECT migration_tools.assert(
        --    NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE ' || quote_ident(foreign_column_name) || ' <> '''' AND x_user IS NULL),
        --    ''Cannot link every barcode'',
        --    ''Every barcode linked''
        --);';

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

-- convenience function for handling desired asset stat cats

CREATE OR REPLACE FUNCTION migration_tools.vivicate_asset_sc_and_sce (TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        field_suffix ALIAS FOR $3; -- for distinguishing between desired_sce1, desired_sce2, etc.
        org_shortname ALIAS FOR $4;
        proceed BOOLEAN;
        org INTEGER;
        org_list INTEGER[];
        sc TEXT;
        sce TEXT;
    BEGIN

        SELECT 'desired_sc' || field_suffix INTO sc;
        SELECT 'desired_sce' || field_suffix INTO sce;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sc;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sc; 
        END IF;
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sce;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sce; 
        END IF;

        SELECT id INTO org FROM actor.org_unit WHERE shortname = org_shortname;
        IF org IS NULL THEN
            RAISE EXCEPTION 'Cannot find org by shortname';
        END IF;
        SELECT INTO org_list ARRAY_ACCUM(id) FROM actor.org_unit_full_path( org );

        -- caller responsible for their own truncates though we try to prevent duplicates
        EXECUTE 'INSERT INTO asset_stat_cat (owner, name)
            SELECT DISTINCT
                 $1
                ,BTRIM('||sc||')
            FROM 
                ' || quote_ident(table_name) || '
            WHERE
                NULLIF(BTRIM('||sc||'),'''') IS NOT NULL
                AND NOT EXISTS (
                    SELECT id
                    FROM asset.stat_cat
                    WHERE owner = ANY ($2)
                    AND name = BTRIM('||sc||')
                )
                AND NOT EXISTS (
                    SELECT id
                    FROM asset_stat_cat
                    WHERE owner = ANY ($2)
                    AND name = BTRIM('||sc||')
                )
            ORDER BY 2;'
        USING org, org_list;

        EXECUTE 'INSERT INTO asset_stat_cat_entry (stat_cat, owner, value)
            SELECT DISTINCT
                COALESCE(
                    (SELECT id
                        FROM asset.stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name))
                   ,(SELECT id
                        FROM asset_stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name))
                )
                ,$1
                ,BTRIM('||sce||')
            FROM 
                ' || quote_ident(table_name) || '
            WHERE
                    NULLIF(BTRIM('||sc||'),'''') IS NOT NULL
                AND NULLIF(BTRIM('||sce||'),'''') IS NOT NULL
                AND NOT EXISTS (
                    SELECT id
                    FROM asset.stat_cat_entry
                    WHERE stat_cat = (
                        SELECT id
                        FROM asset.stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name)
                    ) AND value = BTRIM('||sce||')
                    AND owner = ANY ($2)
                )
                AND NOT EXISTS (
                    SELECT id
                    FROM asset_stat_cat_entry
                    WHERE stat_cat = (
                        SELECT id
                        FROM asset_stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name)
                    ) AND value = BTRIM('||sce||')
                    AND owner = ANY ($2)
                )
            ORDER BY 1,3;'
        USING org, org_list;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.handle_asset_sc_and_sce (TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        field_suffix ALIAS FOR $3; -- for distinguishing between desired_sce1, desired_sce2, etc.
        org_shortname ALIAS FOR $4;
        proceed BOOLEAN;
        org INTEGER;
        org_list INTEGER[];
        o INTEGER;
        sc TEXT;
        sce TEXT;
    BEGIN
        SELECT 'desired_sc' || field_suffix INTO sc;
        SELECT 'desired_sce' || field_suffix INTO sce;
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sc;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sc; 
        END IF;
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sce;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sce; 
        END IF;

        SELECT id INTO org FROM actor.org_unit WHERE shortname = org_shortname;
        IF org IS NULL THEN
            RAISE EXCEPTION 'Cannot find org by shortname';
        END IF;

        SELECT INTO org_list ARRAY_ACCUM(id) FROM actor.org_unit_full_path( org );

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_sc' || field_suffix;
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_sc' || field_suffix || ' INTEGER';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_sce' || field_suffix;
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_sce' || field_suffix || ' INTEGER';


        EXECUTE 'UPDATE ' || quote_ident(table_name) || '
            SET
                x_sc' || field_suffix || ' = id
            FROM
                (SELECT id, name, owner FROM asset_stat_cat
                    UNION SELECT id, name, owner FROM asset.stat_cat) u
            WHERE
                    BTRIM(UPPER(u.name)) = BTRIM(UPPER(' || sc || '))
                AND u.owner = ANY ($1);'
        USING org_list;

        EXECUTE 'UPDATE ' || quote_ident(table_name) || '
            SET
                x_sce' || field_suffix || ' = id
            FROM
                (SELECT id, stat_cat, owner, value FROM asset_stat_cat_entry
                    UNION SELECT id, stat_cat, owner, value FROM asset.stat_cat_entry) u
            WHERE
                    u.stat_cat = x_sc' || field_suffix || '
                AND BTRIM(UPPER(u.value)) = BTRIM(UPPER(' || sce || '))
                AND u.owner = ANY ($1);'
        USING org_list;

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_sc' || field_suffix || ' <> '''' AND x_sc' || field_suffix || ' IS NULL),
            ''Cannot find a desired stat cat'',
            ''Found all desired stat cats''
        );';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_sce' || field_suffix || ' <> '''' AND x_sce' || field_suffix || ' IS NULL),
            ''Cannot find a desired stat cat entry'',
            ''Found all desired stat cat entries''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

DROP FUNCTION IF EXISTS migration_tools.btrim_lcolumns(TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.btrim_lcolumns(s_name TEXT, t_name TEXT) RETURNS BOOLEAN
 LANGUAGE plpgsql
AS $function$
DECLARE
    c_name     TEXT;
BEGIN

    FOR c_name IN SELECT column_name FROM information_schema.columns WHERE 
            table_name = t_name
            AND table_schema = s_name
            AND (data_type='text' OR data_type='character varying')
            AND column_name like 'l_%'
    LOOP
       EXECUTE FORMAT('UPDATE ' || s_name || '.' || t_name || ' SET ' || c_name || ' = BTRIM(' || c_name || ')'); 
    END LOOP;  

    RETURN TRUE;
END
$function$;

DROP FUNCTION IF EXISTS migration_tools.btrim_columns(TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.btrim_columns(s_name TEXT, t_name TEXT) RETURNS BOOLEAN
 LANGUAGE plpgsql
AS $function$
DECLARE
    c_name     TEXT;
BEGIN

    FOR c_name IN SELECT column_name FROM information_schema.columns WHERE 
            table_name = t_name
            AND table_schema = s_name
            AND (data_type='text' OR data_type='character varying')
    LOOP
       EXECUTE FORMAT('UPDATE ' || s_name || '.' || t_name || ' SET ' || c_name || ' = BTRIM(' || c_name || ')'); 
    END LOOP;  

    RETURN TRUE;
END
$function$;

DROP FUNCTION IF EXISTS migration_tools.null_empty_lcolumns(TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.null_empty_lcolumns(s_name TEXT, t_name TEXT) RETURNS BOOLEAN
 LANGUAGE plpgsql
AS $function$
DECLARE
    c_name     TEXT;
BEGIN

    FOR c_name IN SELECT column_name FROM information_schema.columns WHERE 
            table_name = t_name
            AND table_schema = s_name
            AND (data_type='text' OR data_type='character varying')
            AND column_name like 'l_%'
    LOOP
       EXECUTE FORMAT('UPDATE ' || s_name || '.' || t_name || ' SET ' || c_name || ' = NULL WHERE ' || c_name || ' = '''' '); 
    END LOOP;  

    RETURN TRUE;
END
$function$;

DROP FUNCTION IF EXISTS migration_tools.null_empty_columns(TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.null_empty_columns(s_name TEXT, t_name TEXT) RETURNS BOOLEAN
 LANGUAGE plpgsql
AS $function$
DECLARE
    c_name     TEXT;
BEGIN

    FOR c_name IN SELECT column_name FROM information_schema.columns WHERE
            table_name = t_name
            AND table_schema = s_name
            AND (data_type='text' OR data_type='character varying')
    LOOP
       EXECUTE FORMAT('UPDATE ' || s_name || '.' || t_name || ' SET ' || c_name || ' = NULL WHERE ' || c_name || ' = '''' ');
    END LOOP;

    RETURN TRUE;
END
$function$;


-- convenience function for handling item barcode collisions in asset_copy_legacy

CREATE OR REPLACE FUNCTION migration_tools.handle_asset_barcode_collisions(migration_schema TEXT) RETURNS VOID AS $function$
DECLARE
    x_barcode TEXT;
    x_id BIGINT;
    row_count NUMERIC;
    internal_collision_count NUMERIC := 0;
    incumbent_collision_count NUMERIC := 0;
BEGIN
    FOR x_barcode IN SELECT barcode FROM asset_copy_legacy WHERE x_migrate GROUP BY 1 HAVING COUNT(*) > 1
    LOOP
        FOR x_id IN SELECT id FROM asset_copy WHERE barcode = x_barcode
        LOOP
            UPDATE asset_copy SET barcode = migration_schema || '_internal_collision_' || id || '_' || barcode WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            internal_collision_count := internal_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% internal collisions', internal_collision_count;
    FOR x_barcode IN SELECT a.barcode FROM asset.copy a, asset_copy_legacy b WHERE x_migrate AND a.deleted IS FALSE AND a.barcode = b.barcode
    LOOP
        FOR x_id IN SELECT id FROM asset_copy_legacy WHERE barcode = x_barcode
        LOOP
            UPDATE asset_copy_legacy SET barcode = migration_schema || '_incumbent_collision_' || id || '_' || barcode WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            incumbent_collision_count := incumbent_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% incumbent collisions', incumbent_collision_count;
END
$function$ LANGUAGE plpgsql;

-- convenience function for handling patron barcode/usrname collisions in actor_usr_legacy
-- this should be ran prior to populating actor_card

CREATE OR REPLACE FUNCTION migration_tools.handle_actor_barcode_collisions(migration_schema TEXT) RETURNS VOID AS $function$
DECLARE
    x_barcode TEXT;
    x_id BIGINT;
    row_count NUMERIC;
    internal_collision_count NUMERIC := 0;
    incumbent_barcode_collision_count NUMERIC := 0;
    incumbent_usrname_collision_count NUMERIC := 0;
BEGIN
    FOR x_barcode IN SELECT usrname FROM actor_usr_legacy WHERE x_migrate GROUP BY 1 HAVING COUNT(*) > 1
    LOOP
        FOR x_id IN SELECT id FROM actor_usr_legacy WHERE x_migrate AND usrname = x_barcode
        LOOP
            UPDATE actor_usr_legacy SET usrname = migration_schema || '_internal_collision_' || id || '_' || usrname WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            internal_collision_count := internal_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% internal usrname/barcode collisions', internal_collision_count;

    FOR x_barcode IN
        SELECT a.barcode FROM actor.card a, actor_usr_legacy b WHERE x_migrate AND a.barcode = b.usrname
    LOOP
        FOR x_id IN SELECT DISTINCT id FROM actor_usr_legacy WHERE x_migrate AND usrname = x_barcode
        LOOP
            UPDATE actor_usr_legacy SET usrname = migration_schema || '_incumbent_barcode_collision_' || id || '_' || usrname WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            incumbent_barcode_collision_count := incumbent_barcode_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% incumbent barcode collisions', incumbent_barcode_collision_count;

    FOR x_barcode IN
        SELECT a.usrname FROM actor.usr a, actor_usr_legacy b WHERE x_migrate AND a.usrname = b.usrname
    LOOP
        FOR x_id IN SELECT DISTINCT id FROM actor_usr_legacy WHERE x_migrate AND usrname = x_barcode
        LOOP
            UPDATE actor_usr_legacy SET usrname = migration_schema || '_incumbent_usrname_collision_' || id || '_' || usrname WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            incumbent_usrname_collision_count := incumbent_usrname_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% incumbent usrname collisions (post barcode collision munging)', incumbent_usrname_collision_count;
END
$function$ LANGUAGE plpgsql;
