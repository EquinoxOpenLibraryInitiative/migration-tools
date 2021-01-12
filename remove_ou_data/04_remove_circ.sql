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

ALTER TABLE action.circulation DISABLE TRIGGER action_circulation_aging_tgr;
ALTER TABLE action.circulation DISABLE TRIGGER age_parent_circ;
ALTER TABLE action.circulation_limit_group_map DROP CONSTRAINT circulation_limit_group_map_circ_fkey;
ALTER TABLE action.usr_circ_history DROP CONSTRAINT usr_circ_history_source_circ_fkey;

BEGIN;

UPDATE action.circulation 
SET parent_circ = NULL   
WHERE parent_circ IN 
(SELECT id FROM action.circulation WHERE circ_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM action.circulation WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM action.circulation WHERE circ_staff IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM action.circulation WHERE circ_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM action.aged_circulation WHERE circ_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM action.aged_circulation WHERE copy_circ_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM action.aged_circulation WHERE copy_owning_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

DELETE FROM action.non_cat_in_house_use WHERE org_unit IN 
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

UPDATE action.non_cat_in_house_use SET staff = 1 WHERE staff IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del))
AND org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

DELETE FROM action.usr_circ_history WHERE id IN (SELECT a.id FROM action.usr_circ_history a LEFT JOIN action.circulation b ON b.id = a.source_circ WHERE b.id IS NULL);

DELETE FROM action.circulation_limit_group_map WHERE circ IN (SELECT a.circ FROM action.circulation_limit_group_map a LEFT JOIN action.circulation b ON b.id = a.circ WHERE b.id IS NULL);

COMMIT;

ALTER TABLE action.circulation ENABLE TRIGGER action_circulation_aging_tgr;
ALTER TABLE action.circulation ENABLE TRIGGER age_parent_circ;
ALTER TABLE action.usr_circ_history ADD CONSTRAINT usr_circ_history_source_circ_fkey FOREIGN KEY (source_circ) REFERENCES action.circulation(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE action.circulation_limit_group_map ADD CONSTRAINT circulation_limit_group_map_circ_fkey FOREIGN KEY (circ) REFERENCES action.circulation(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;

