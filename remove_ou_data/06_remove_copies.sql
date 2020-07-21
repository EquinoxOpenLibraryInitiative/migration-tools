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
\set ECHO all
\timing

ALTER TABLE asset.copy DISABLE RULE protect_copy_delete;
ALTER TABLE asset.copy DISABLE TRIGGER audit_asset_copy_update_trigger;
CREATE INDEX tmp_import_as ON vandelay.import_item(imported_as);

BEGIN;

-- NOTE: no FK
DELETE FROM asset.opac_visible_copies WHERE copy_id IN (
  SELECT id FROM asset.copy WHERE circ_lib IN
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
);
-- NOTE: no FK
DELETE FROM asset.copy_part_map WHERE target_copy IN (
  SELECT id FROM asset.copy WHERE circ_lib IN
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
);

-- NOTE: no FK
DELETE FROM asset.stat_cat_entry_copy_map WHERE owning_copy IN (
  SELECT id FROM asset.copy WHERE circ_lib IN
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
);

DELETE FROM asset.copy_note WHERE owning_copy IN (
  SELECT id FROM asset.copy WHERE circ_lib IN
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
);

DELETE FROM vandelay.import_item WHERE imported_as IN (
  SELECT id FROM asset.copy WHERE circ_lib IN
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
);

DELETE FROM asset.copy WHERE circ_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

DELETE FROM asset.latest_inventory WHERE copy IN 
    (SELECT li.copy FROM asset.latest_inventory li LEFT JOIN asset.copy acp ON acp.id = li.copy WHERE acp.id IS NULL)
;

COMMIT;

DROP INDEX vandelay.tmp_import_as;
ALTER TABLE asset.copy ENABLE RULE protect_copy_delete;
ALTER TABLE asset.copy ENABLE TRIGGER audit_asset_copy_update_trigger;
