
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

-- FIXME: testing for STAFF_LOGIN perm is probably better
CREATE OR REPLACE FUNCTION migration_tools.is_staff_profile (INT) RETURNS BOOLEAN AS $$
  DECLARE
    profile ALIAS FOR $1;
  BEGIN
    RETURN CASE WHEN 'Staff' IN (select (permission.grp_ancestors(profile)).name) THEN TRUE ELSE FALSE END;
  END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

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
