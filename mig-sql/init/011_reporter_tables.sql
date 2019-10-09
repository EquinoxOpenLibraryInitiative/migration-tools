CREATE TABLE report (id SERIAL, create_date TIMESTAMPTZ, name TEXT);
CREATE TABLE reporter_columns (id SERIAL, report INTEGER, header TEXT, ordinal_position INTEGER);
CREATE TABLE reporter_rows (id SERIAL, report INTEGER, row INTEGER, ordinal_position INTEGER);


