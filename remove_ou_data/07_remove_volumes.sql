-- Copyright 2015, Equinox Software, Inc.
-- Author: Galen Charlton <gmc@esilibrary.com>
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

\set ou_to_del ''''EXAMPLE''''
\set vol_del_table ORGUNIT_volume_bibs
\set ECHO all
\timing

DROP TABLE IF EXISTS esi.:vol_del_table;

ALTER TABLE asset.call_number DISABLE RULE protect_cn_delete;
ALTER TABLE asset.call_number DISABLE TRIGGER audit_asset_call_number_update_trigger;

BEGIN;

DELETE FROM asset.uri_call_number_map WHERE call_number IN (
    SELECT id FROM asset.call_number WHERE owning_lib IN
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
);

CREATE TABLE esi.:vol_del_table AS SELECT DISTINCT record
FROM asset.call_number WHERE owning_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

DELETE FROM asset.call_number WHERE owning_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

DELETE FROM asset.call_number_prefix WHERE owning_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

DELETE FROM asset.call_number_suffix WHERE owning_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

COMMIT;

ALTER TABLE asset.call_number ENABLE RULE protect_cn_delete;
ALTER TABLE asset.call_number ENABLE TRIGGER audit_asset_call_number_update_trigger;

CREATE INDEX org_vol_bib_idx ON esi.:vol_del_table(record);
