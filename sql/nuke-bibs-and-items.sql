-- Copyright 2009-2012, Equinox Software, Inc.
--
-- This program is free software; you can redistribute it and/or
-- modify it under the terms of the GNU General Public License
-- as published by the Free Software Foundation; either version 2
-- of the License, or (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

BEGIN;

DROP TABLE IF EXISTS asset.call_number_temp;
CREATE TABLE asset.call_number_temp AS (
  SELECT * FROM asset.call_number WHERE id = -1
);

DROP TABLE IF EXISTS biblio.record_entry_temp;
CREATE TABLE biblio.record_entry_temp AS (
  SELECT * FROM biblio.record_entry WHERE id = -1
);


TRUNCATE
  action.circulation,
  asset.copy,
  asset.opac_visible_copies,
  biblio.record_entry,
  asset.call_number,
  metabib.metarecord_source_map,
  metabib.metarecord,
  metabib.facet_entry,
  metabib.browse_entry,
  authority.record_entry,
  authority.full_rec,
  authority.rec_descriptor
CASCADE;

INSERT INTO asset.call_number SELECT * FROM asset.call_number_temp;
INSERT INTO biblio.record_entry SELECT * FROM biblio.record_entry_temp;

DROP TABLE asset.call_number_temp;
DROP TABLE biblio.record_entry_temp;

\echo If the purge looks good, please COMMIT
\echo You should also follow with a VACUUM ANALYZE or the like.

