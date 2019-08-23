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
