
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
