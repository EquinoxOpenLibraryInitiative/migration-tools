
DROP TABLE IF EXISTS migration_tools.ils_holdings;
CREATE TABLE migration_tools.ils_holdings (
     id                        SERIAL
    ,ils                       TEXT UNIQUE
    ,tag                       VARCHAR(3)
);

DROP TABLE IF EXISTS migration_tools.ils_holding_fields;
CREATE TABLE migration_tools.ils_holding_fields (
     id           SERIAL
    ,ils          TEXT REFERENCES migration_tools.ils_holdings (ils)
    ,subfield     VARCHAR(1)
    ,label        VARCHAR(50)
    ,repeatable   BOOLEAN DEFAULT FALSE 
);

INSERT INTO migration_tools.ils_holdings (ils,tag) VALUES 
('atrium','852');

INSERT INTO migration_tools.ils_holding_fields (ils,subfield,label) VALUES 
('atrium','p','barcode') ,('atrium','9','price') ,('atrium','b','physical_location')
,('atrium','a','current_location') ,('atrium','k','call_number_prefix') ,('atrium','h','call_number_label')
,('atrium','7','item_circulation_class') ,('atrium','4','material_type');


DROP FUNCTION IF EXISTS migration_tools.add_marc_holdings_fields(TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.add_marc_holdings_fields (schema TEXT, target_ils TEXT) 
    RETURNS BOOLEAN 
    LANGUAGE plpgsql
AS $function$
DECLARE 
    holding_label    TEXT;
    copy_table       TEXT := schema || '.m_asset_copy_legacy';
    field_repeatable BOOLEAN;
BEGIN
    IF NOT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = ''' || schema || ''' AND table_name = 'm_asset_copy_legacy') THEN 
        EXECUTE ('CREATE TABLE ' || copy_table || ' (x_migrate BOOLEAN DEFAULT TRUE) INHERITS (' || schema || '.m_asset_copy);');
    END IF;
    EXECUTE ('ALTER TABLE ' || copy_table || ' ADD COLUMN x_bre_id BIGINT;' );
    EXECUTE ('ALTER TABLE ' || copy_table || ' ADD COLUMN l_xml TEXT;' );
    FOR holding_label IN EXECUTE ('SELECT label FROM migration_tools.ils_holding_fields WHERE ils = ''' || target_ils || ''';' ) LOOP
        EXECUTE ('SELECT repeatable FROM migration_tools.ils_holding_fields WHERE ils = ''' || target_ils || ''' AND label = ''' || holding_label || ''';') INTO field_repeatable;
        IF field_repeatable THEN 
            EXECUTE ('ALTER TABLE ' || copy_table || ' ADD COLUMN l_' || holding_label || ' TEXT[];' );
        ELSE 
            EXECUTE ('ALTER TABLE ' || copy_table || ' ADD COLUMN l_' || holding_label || ' TEXT;' );
        END IF;
    END LOOP;
    RETURN TRUE;
END;
$function$
;

DROP FUNCTION IF EXISTS migration_tools.extract_holdings_from_marc(BIGINT,TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.extract_holdings_from_marc (bre_id BIGINT, schema TEXT, target_ils TEXT) 
    RETURNS BIGINT 
    LANGUAGE plpgsql
AS $function$
DECLARE 
    bre_marc     TEXT;
    bre_table    TEXT := schema || '.m_biblio_record_entry';
    acp_table    TEXT := schema || '.m_asset_copy_legacy';
    copy_xml     TEXT;
    holding_tag  VARCHAR(3);
    copy_id      BIGINT;
    copy_subfield      VARCHAR(1);
    copy_label         TEXT;
    copy_value         TEXT;
    copy_value_array   TEXT[];
    copy_repeatable    BOOLEAN;
BEGIN
    EXECUTE ('SELECT marc from ' ||  bre_table || ' WHERE id = ' || bre_id || ';') INTO bre_marc;
    EXECUTE ('SELECT tag FROM migration_tools.ils_holdings WHERE ils = ''' || target_ils || ''';') INTO holding_tag;
    FOR copy_xml IN EXECUTE ('SELECT UNNEST(oils_xpath(''//*[@tag="' || holding_tag || '"]'',$_$' || bre_marc || '$_$));' ) LOOP
        EXECUTE ('INSERT INTO ' ||  acp_table || ' (x_bre_id,l_xml) VALUES (' || bre_id || ',$_$' || copy_xml || '$_$) RETURNING id;') INTO copy_id;    
        FOR copy_subfield IN SELECT subfield, label, repeatable FROM migration_tools.ils_holding_fields WHERE ils = target_ils LOOP
            SELECT label, repeatable FROM migration_tools.ils_holding_fields WHERE ils = target_ils AND subfield = copy_subfield INTO copy_label, copy_repeatable;
            IF copy_repeatable THEN 
                EXECUTE ('SELECT oils_xpath( ''//*[@tag="' || holding_tag || '"]/*[@code="' || copy_subfield || '"]/text()'', '''|| copy_xml || ''');') INTO copy_value_array;
                IF copy_value_array IS NOT NULL THEN 
                    EXECUTE ('UPDATE ' || acp_table || ' SET l_' || copy_label || ' = ''' || copy_value_array || ''' WHERE id = ' || copy_id || ';');
                END IF;
            ELSE 
                EXECUTE ('SELECT UNNEST(oils_xpath( ''//*[@tag="' || holding_tag || '"]/*[@code="' || copy_subfield || '"]/text()'', $_$' || copy_xml || '$_$));') INTO copy_value;
                IF copy_value IS NOT NULL THEN 
                    EXECUTE ('UPDATE ' || acp_table || ' SET l_' || copy_label || ' = $_$' || copy_value || '$_$ WHERE id = ' || copy_id || ';');
                END IF;
            END IF;
        END LOOP;
    END LOOP;
    RETURN bre_id;
END;
$function$
;

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
