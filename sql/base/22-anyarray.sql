CREATE FUNCTION migration_tools.anyarray_agg_statefunc(state anyarray, value anyarray)
        RETURNS anyarray AS
$BODY$
        SELECT array_cat($1, $2)
$BODY$
        LANGUAGE sql IMMUTABLE;

DROP AGGREGATE IF EXISTS anyarray_agg(anyarray);
CREATE AGGREGATE anyarray_agg(anyarray) (
    SFUNC = migration_tools.anyarray_agg_statefunc,
    STYPE = anyarray
);

DROP FUNCTION IF EXISTS migration_tools.anyarray_sort(anyarray);
CREATE OR REPLACE FUNCTION migration_tools.anyarray_sort(with_array anyarray)
    RETURNS anyarray AS
$BODY$
    DECLARE
        return_array with_array%TYPE := '{}';
    BEGIN
        SELECT ARRAY_AGG(sorted_vals.val) AS array_value
        FROM
            (   SELECT UNNEST(with_array) AS val
                ORDER BY val
            ) AS sorted_vals INTO return_array;
        RETURN return_array;
    END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS migration_tools.anyarray_uniq(anyarray);
CREATE OR REPLACE FUNCTION migration_tools.anyarray_uniq(with_array anyarray)
    RETURNS anyarray AS
$BODY$
    DECLARE
        -- The variable used to track iteration over "with_array".
        loop_offset integer;

        -- The array to be returned by this function.
        return_array with_array%TYPE := '{}';
    BEGIN
        IF with_array IS NULL THEN
            return NULL;
        END IF;

        IF with_array = '{}' THEN
            return return_array;
        END IF;

        -- Iterate over each element in "concat_array".
        FOR loop_offset IN ARRAY_LOWER(with_array, 1)..ARRAY_UPPER(with_array, 1) LOOP
            IF with_array[loop_offset] IS NULL THEN
                IF NOT EXISTS
                    ( SELECT 1 FROM UNNEST(return_array) AS s(a)
                    WHERE a IS NULL )
                THEN return_array = ARRAY_APPEND(return_array, with_array[loop_offset]);
                END IF;
            -- When an array contains a NULL value, ANY() returns NULL instead of FALSE...
            ELSEIF NOT(with_array[loop_offset] = ANY(return_array)) OR NOT(NULL IS DISTINCT FROM (with_array[loop_offset] = ANY(return_array))) THEN
                return_array = ARRAY_APPEND(return_array, with_array[loop_offset]);
            END IF;
        END LOOP;

    RETURN return_array;
 END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS migration_tools.anyarray_concat(anyarray, anyarray);
CREATE OR REPLACE FUNCTION migration_tools.anyarray_concat(with_array anyarray, concat_array anyarray)
	RETURNS anyarray AS
$BODY$
	DECLARE
		-- The variable used to track iteration over "with_array".
		loop_offset integer;

		-- The array to be returned by this function.
		return_array with_array%TYPE;
	BEGIN
		IF with_array IS NULL THEN
			RETURN concat_array;
		ELSEIF concat_array IS NULL THEN
			RETURN with_array;
		END IF;

		-- Add all items in "with_array" to "return_array".
		return_array = with_array;

		-- Iterate over each element in "concat_array", appending it to "return_array".
		FOR loop_offset IN ARRAY_LOWER(concat_array, 1)..ARRAY_UPPER(concat_array, 1) LOOP
			return_array = ARRAY_APPEND(return_array, concat_array[loop_offset]);
		END LOOP;

		RETURN return_array;
	END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS migration_tools.anyarray_concat(anyarray, anynonarray);
CREATE OR REPLACE FUNCTION migration_tools.anyarray_concat(with_array anyarray, concat_element anynonarray)
	RETURNS anyarray AS
$BODY$
	BEGIN
		RETURN ANYARRAY_CONCAT(with_array, ARRAY[concat_element]);
	END;
$BODY$ LANGUAGE plpgsql;
DROP FUNCTION IF EXISTS migration_tools.anyarray_concat_uniq(anyarray, anyarray);
CREATE OR REPLACE FUNCTION migration_tools.anyarray_concat_uniq(with_array anyarray, concat_array anyarray)
	RETURNS anyarray AS
$BODY$
	DECLARE
		-- The variable used to track iteration over "with_array".
		loop_offset integer;

		-- The array to be returned by this function.
		return_array with_array%TYPE;
	BEGIN
		IF with_array IS NULL THEN
			RETURN concat_array;
		ELSEIF concat_array IS NULL THEN
			RETURN with_array;
		END IF;

		-- Add all items in "with_array" to "return_array".
		return_array = with_array;

		-- Iterate over each element in "concat_array".
		FOR loop_offset IN ARRAY_LOWER(concat_array, 1)..ARRAY_UPPER(concat_array, 1) LOOP
			IF NOT concat_array[loop_offset] = ANY(return_array) THEN
				return_array = ARRAY_APPEND(return_array, concat_array[loop_offset]);
			END IF;
		END LOOP;

		RETURN return_array;
	END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS migration_tools.anyarray_concat_uniq(anyarray, anynonarray);
CREATE OR REPLACE FUNCTION migration_tools.anyarray_concat_uniq(with_array anyarray, concat_element anynonarray)
	RETURNS anyarray AS
$BODY$
	BEGIN
		RETURN ANYARRAY_CONCAT_UNIQ(with_array, ARRAY[concat_element]);
	END;
$BODY$ LANGUAGE plpgsql;
DROP FUNCTION IF EXISTS migration_tools.anyarray_diff(anyarray, anyarray);
CREATE OR REPLACE FUNCTION migration_tools.anyarray_diff(with_array anyarray, against_array anyarray)
	RETURNS anyarray AS
$BODY$
	DECLARE
		-- The variable used to track iteration over "with_array".
		loop_offset integer;
		
		-- The array to be returned by this function.
		return_array with_array%TYPE := '{}';
	BEGIN
		IF with_array IS NULL THEN
			RETURN against_array;
		ELSEIF against_array IS NULL THEN
			RETURN with_array;
		END IF;

		-- Iterate over with_array.
		FOR loop_offset IN ARRAY_LOWER(with_array, 1)..ARRAY_UPPER(with_array, 1) LOOP
			IF NOT with_array[loop_offset] = ANY(against_array) THEN
				return_array = ARRAY_APPEND(return_array, with_array[loop_offset]);
			END IF;
		END LOOP;

		-- Iterate over against_array.
		FOR loop_offset IN ARRAY_LOWER(against_array, 1)..ARRAY_UPPER(against_array, 1) LOOP
			IF NOT against_array[loop_offset] = ANY(with_array) THEN
				return_array = ARRAY_APPEND(return_array, against_array[loop_offset]);
			END IF;
		END LOOP;

		RETURN return_array;
	END;
$BODY$ LANGUAGE plpgsql;
DROP FUNCTION IF EXISTS migration_tools.anyarray_diff_uniq(anyarray, anyarray);
CREATE OR REPLACE FUNCTION migration_tools.anyarray_diff_uniq(with_array anyarray, against_array anyarray)
	RETURNS anyarray AS
$BODY$
	DECLARE
		-- The variable used to track iteration over "with_array".
		loop_offset integer;
		
		-- The array to be returned by this function.
		return_array with_array%TYPE := '{}';
	BEGIN
		IF with_array IS NULL THEN
			RETURN against_array;
		ELSEIF against_array IS NULL THEN
			RETURN with_array;
		END IF;

		-- Iterate over with_array.
		FOR loop_offset IN ARRAY_LOWER(with_array, 1)..ARRAY_UPPER(with_array, 1) LOOP
			RAISE NOTICE '% %', with_array[loop_offset], return_array;
			IF (NOT with_array[loop_offset] = ANY(against_array)) AND (NOT with_array[loop_offset] = ANY(return_array)) THEN
				return_array = ARRAY_APPEND(return_array, with_array[loop_offset]);
			END IF;
		END LOOP;

		-- Iterate over against_array.
		FOR loop_offset IN ARRAY_LOWER(against_array, 1)..ARRAY_UPPER(against_array, 1) LOOP
			RAISE NOTICE '% %', against_array[loop_offset], return_array;
			IF (NOT against_array[loop_offset] = ANY(with_array)) AND (NOT against_array[loop_offset] = ANY(return_array)) THEN
				return_array = ARRAY_APPEND(return_array, against_array[loop_offset]);
			END IF;
		END LOOP;

		RETURN return_array;
	END;
$BODY$ LANGUAGE plpgsql;
DROP FUNCTION IF EXISTS migration_tools.anyarray_enumerate(anyarray);
CREATE FUNCTION migration_tools.anyarray_enumerate(anyarray)
	RETURNS TABLE (index bigint, value anyelement) AS
$$
	SELECT
		row_number() OVER (),
		value
	FROM (
		SELECT unnest($1) AS value
	) AS unnested
$$
	LANGUAGE sql IMMUTABLE;
COMMENT ON FUNCTION migration_tools.anyarray_enumerate(anyarray) IS '
Unnests the array along with the indices of each element.

*index* (bigint) is the index of the element within the array starting at 1.

*value* (anyelement) is the element from the array.

NOTE: Multi-dimensional arrays will be flattened as they are with *unnest()*. 
';
DROP FUNCTION IF EXISTS migration_tools.anyarray_is_array(anyelement);
CREATE OR REPLACE FUNCTION migration_tools.anyarray_is_array(anyelement)
	RETURNS boolean AS
$BODY$
	BEGIN
		-- TODO: Is there a more "elegant" / less hacky of accomplishing
		-- this?

		-- If the following function call throws an exception, we know the
		-- element is not an array. If the call succeeds, then it must be
		-- an array.
		EXECUTE FORMAT('WITH a AS (SELECT %L::TEXT[] AS val) SELECT ARRAY_DIMS(a.val) FROM a', $1);
		RETURN TRUE;
	EXCEPTION WHEN
	      SQLSTATE '42804' -- Unknown data-type passed
	      OR SQLSTATE '42883' -- Function doesn't exist
	      OR SQLSTATE '22P02' -- Unable to cast to an array
	THEN
		RETURN FALSE;
	END;
$BODY$ LANGUAGE plpgsql;
DROP FUNCTION IF EXISTS migration_tools.anyarray_numeric_only(anyarray);
CREATE OR REPLACE FUNCTION migration_tools.anyarray_numeric_only(anyarray)
        RETURNS anyarray AS
$BODY$
        SELECT ARRAY(
                SELECT
                        array_values.array_value
                FROM
                        (
                                SELECT UNNEST($1) AS array_value
                        ) AS array_values
                WHERE
                        array_values.array_value::TEXT ~ '^\d+(\.\d+)?$'
        )
$BODY$ LANGUAGE sql IMMUTABLE;
DROP FUNCTION IF EXISTS migration_tools.anyarray_ranges(anyarray);
CREATE OR REPLACE FUNCTION migration_tools.anyarray_ranges(from_array anyarray)
	RETURNS SETOF text[] AS
$BODY$
	BEGIN
		RETURN QUERY SELECT
				ARRAY_AGG(consolidated_values.consolidated_range) AS ranges
			FROM
				(
					SELECT
						(CASE WHEN COUNT(*) > 1 THEN
							MIN(unconsolidated_values.array_value)::text || '-' || MAX(unconsolidated_values.array_value)::text
						ELSE
							MIN(unconsolidated_values.array_value)::text
						END) AS consolidated_range
					FROM
						(
							SELECT
								array_values.array_value,
								ROW_NUMBER() OVER (ORDER BY array_values.array_value) - array_values.array_value AS consolidation_group
							FROM
								(
									SELECT
										UNNEST(from_array) AS array_value
								) AS array_values
							ORDER BY
								array_values.array_value
						) AS unconsolidated_values
					GROUP BY
						unconsolidated_values.consolidation_group
					ORDER BY
						MIN(unconsolidated_values.array_value)
				) AS consolidated_values
		;
	END;
$BODY$ LANGUAGE plpgsql;
DROP FUNCTION IF EXISTS migration_tools.anyarray_remove_null(anyarray);
CREATE OR REPLACE FUNCTION migration_tools.anyarray_remove_null(from_array anyarray)
        RETURNS anyarray AS
$BODY$
        DECLARE
                -- The variable used to track iteration over "from_array".
                loop_offset integer;

                -- The array to be returned by this function.
                return_array from_array%TYPE;
        BEGIN
                -- Iterate over each element in "from_array".
                FOR loop_offset IN ARRAY_LOWER(from_array, 1)..ARRAY_UPPER(from_array, 1) LOOP
                        IF from_array[loop_offset] IS NOT NULL THEN -- If NULL, will omit from "return_array".
                                return_array = ARRAY_APPEND(return_array, from_array[loop_offset]);
                        END IF;
                END LOOP;

                RETURN return_array;
        END;
$BODY$ LANGUAGE plpgsql;
DROP FUNCTION IF EXISTS migration_tools.anyarray_remove(anyarray, anyarray);
CREATE OR REPLACE FUNCTION migration_tools.anyarray_remove(from_array anyarray, remove_array anyarray)
        RETURNS anyarray AS
$BODY$
        DECLARE
                -- The variable used to track iteration over "from_array".
                loop_offset integer;


                -- The array to be returned by this function.
                return_array from_array%TYPE := '{}';
        BEGIN
                -- If either argument is NULL, there is nothing to do.
                IF from_array IS NULL OR remove_array IS NULL THEN
                        RETURN from_array;
                END IF;

                -- Iterate over each element in "from_array".
                FOR loop_offset IN ARRAY_LOWER(from_array, 1)..ARRAY_UPPER(from_array, 1) LOOP
                        -- If the element being iterated over is in "remove_array",
                        -- do not append it to "return_array".
                        IF (from_array[loop_offset] = ANY(remove_array)) IS DISTINCT FROM TRUE THEN
                                return_array = ARRAY_APPEND(return_array, from_array[loop_offset]);
                        END IF;
                END LOOP;


                RETURN return_array;
        END;
$BODY$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS migration_tools.anyarray_remove(anyarray, anynonarray);
CREATE OR REPLACE FUNCTION migration_tools.anyarray_remove(from_array anyarray, remove_element anynonarray)
        RETURNS anyarray AS
$BODY$
        BEGIN
                RETURN ANYARRAY_REMOVE(from_array, ARRAY[remove_element]);
        END;
$BODY$ LANGUAGE plpgsql;
