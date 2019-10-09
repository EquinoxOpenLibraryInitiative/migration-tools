
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

-- internal function for handle_shelf

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

-- convenience functions for handling copy_location maps
CREATE OR REPLACE FUNCTION migration_tools.handle_shelf (TEXT,TEXT,TEXT,INTEGER) RETURNS VOID AS $$
    SELECT migration_tools._handle_shelf($1,$2,$3,$4,TRUE);
$$ LANGUAGE SQL;

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

-- alternate version: convenience function for handling item barcode collisions in asset_copy_legacy

CREATE OR REPLACE FUNCTION migration_tools.handle_asset_barcode_collisions2(migration_schema TEXT) RETURNS VOID AS $function$
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
            UPDATE asset_copy_legacy SET barcode = migration_schema || '_' || barcode WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            incumbent_collision_count := incumbent_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% incumbent collisions', incumbent_collision_count;
END
$function$ LANGUAGE plpgsql;

-- alternate version: convenience function for handling patron barcode/usrname collisions in actor_usr_legacy
-- this should be ran prior to populating actor_card

CREATE OR REPLACE FUNCTION migration_tools.handle_actor_barcode_collisions2(migration_schema TEXT) RETURNS VOID AS $function$
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
            UPDATE actor_usr_legacy SET usrname = migration_schema || '_' || usrname WHERE id = x_id;
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
            UPDATE actor_usr_legacy SET usrname = migration_schema || '_' || usrname WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            incumbent_usrname_collision_count := incumbent_usrname_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% incumbent usrname collisions (post barcode collision munging)', incumbent_usrname_collision_count;
END
$function$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migration_tools.is_circ_rule_safe_to_delete( test_matchpoint INTEGER ) RETURNS BOOLEAN AS $func$
-- WARNING: Use at your own risk
-- FIXME: not considering marc_type, marc_form, marc_bib_level, marc_vr_format, usr_age_lower_bound, usr_age_upper_bound, item_age
DECLARE
    item_object asset.copy%ROWTYPE;
    user_object actor.usr%ROWTYPE;
    test_rule_object config.circ_matrix_matchpoint%ROWTYPE;
    result_rule_object config.circ_matrix_matchpoint%ROWTYPE;
    safe_to_delete BOOLEAN := FALSE;
    m action.found_circ_matrix_matchpoint;
    n action.found_circ_matrix_matchpoint;
    -- ( success BOOL, matchpoint config.circ_matrix_matchpoint, buildrows INT[] )
    result_matchpoint INTEGER;
BEGIN
    SELECT INTO test_rule_object * FROM config.circ_matrix_matchpoint WHERE id = test_matchpoint;
    RAISE INFO 'testing rule: %', test_rule_object;

    INSERT INTO actor.usr (
        profile,
        usrname,
        passwd,
        ident_type,
        first_given_name,
        family_name,
        home_ou,
        juvenile
    ) SELECT
        COALESCE(test_rule_object.grp, 2),
        'is_circ_rule_safe_to_delete_' || test_matchpoint || '_' || NOW()::text,
        MD5(NOW()::TEXT),
        1,
        'Ima',
        'Test',
        COALESCE(test_rule_object.user_home_ou, test_rule_object.org_unit),
        COALESCE(test_rule_object.juvenile_flag, FALSE)
    ;
    
    SELECT INTO user_object * FROM actor.usr WHERE id = currval('actor.usr_id_seq');

    INSERT INTO asset.call_number (
        creator,
        editor,
        record,
        owning_lib,
        label,
        label_class
    ) SELECT
        1,
        1,
        -1,
        COALESCE(test_rule_object.copy_owning_lib,test_rule_object.org_unit),
        'is_circ_rule_safe_to_delete_' || test_matchpoint || '_' || NOW()::text,
        1
    ;

    INSERT INTO asset.copy (
        barcode,
        circ_lib,
        creator,
        call_number,
        editor,
        location,
        loan_duration,
        fine_level,
        ref,
        circ_modifier
    ) SELECT
        'is_circ_rule_safe_to_delete_' || test_matchpoint || '_' || NOW()::text,
        COALESCE(test_rule_object.copy_circ_lib,test_rule_object.org_unit),
        1,
        currval('asset.call_number_id_seq'),
        1,
        COALESCE(test_rule_object.copy_location,1),
        2,
        2,
        COALESCE(test_rule_object.ref_flag,FALSE),
        test_rule_object.circ_modifier
    ;

    SELECT INTO item_object * FROM asset.copy WHERE id = currval('asset.copy_id_seq');

    SELECT INTO m * FROM action.find_circ_matrix_matchpoint(
        test_rule_object.org_unit,
        item_object,
        user_object,
        COALESCE(test_rule_object.is_renewal,FALSE)
    );
    RAISE INFO '   action.find_circ_matrix_matchpoint(%,%,%,%) = (%,%,%)',
        test_rule_object.org_unit,
        item_object.id,
        user_object.id,
        COALESCE(test_rule_object.is_renewal,FALSE),
        m.success,
        m.matchpoint,
        m.buildrows
    ;

    --  disable the rule being tested to see if the outcome changes
    UPDATE config.circ_matrix_matchpoint SET active = FALSE WHERE id = (m.matchpoint).id;

    SELECT INTO n * FROM action.find_circ_matrix_matchpoint(
        test_rule_object.org_unit,
        item_object,
        user_object,
        COALESCE(test_rule_object.is_renewal,FALSE)
    );
    RAISE INFO 'VS action.find_circ_matrix_matchpoint(%,%,%,%) = (%,%,%)',
        test_rule_object.org_unit,
        item_object.id,
        user_object.id,
        COALESCE(test_rule_object.is_renewal,FALSE),
        n.success,
        n.matchpoint,
        n.buildrows
    ;

    -- FIXME: We could dig deeper and see if the referenced config.rule_*
    -- entries are effectively equivalent, but for now, let's assume no
    -- duplicate rules at that level
    IF (
            (m.matchpoint).circulate = (n.matchpoint).circulate
        AND (m.matchpoint).duration_rule = (n.matchpoint).duration_rule
        AND (m.matchpoint).recurring_fine_rule = (n.matchpoint).recurring_fine_rule
        AND (m.matchpoint).max_fine_rule = (n.matchpoint).max_fine_rule
        AND (
                (m.matchpoint).hard_due_date = (n.matchpoint).hard_due_date
                OR (
                        (m.matchpoint).hard_due_date IS NULL
                    AND (n.matchpoint).hard_due_date IS NULL
                )
        )
        AND (
                (m.matchpoint).renewals = (n.matchpoint).renewals
                OR (
                        (m.matchpoint).renewals IS NULL
                    AND (n.matchpoint).renewals IS NULL
                )
        )
        AND (
                (m.matchpoint).grace_period = (n.matchpoint).grace_period
                OR (
                        (m.matchpoint).grace_period IS NULL
                    AND (n.matchpoint).grace_period IS NULL
                )
        )
        AND (
                (m.matchpoint).total_copy_hold_ratio = (n.matchpoint).total_copy_hold_ratio
                OR (
                        (m.matchpoint).total_copy_hold_ratio IS NULL
                    AND (n.matchpoint).total_copy_hold_ratio IS NULL
                )
        )
        AND (
                (m.matchpoint).available_copy_hold_ratio = (n.matchpoint).available_copy_hold_ratio
                OR (
                        (m.matchpoint).available_copy_hold_ratio IS NULL
                    AND (n.matchpoint).available_copy_hold_ratio IS NULL
                )
        )
        AND NOT EXISTS (
            SELECT limit_set, fallthrough
            FROM config.circ_matrix_limit_set_map
            WHERE active and matchpoint = (m.matchpoint).id
            EXCEPT
            SELECT limit_set, fallthrough
            FROM config.circ_matrix_limit_set_map
            WHERE active and matchpoint = (n.matchpoint).id
        )

    ) THEN
        RAISE INFO 'rule has same outcome';
        safe_to_delete := TRUE;
    ELSE
        RAISE INFO 'rule has different outcome';
        safe_to_delete := FALSE;
    END IF;

    RAISE EXCEPTION 'rollback the temporary changes';

EXCEPTION WHEN OTHERS THEN

    RAISE INFO 'inside exception block: %, %', SQLSTATE, SQLERRM;
    RETURN safe_to_delete;

END;
$func$ LANGUAGE plpgsql;

