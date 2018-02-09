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

ALTER TABLE serial.record_entry DISABLE RULE protect_mfhd_delete;

BEGIN;

DELETE FROM serial.basic_summary WHERE distribution IN 
(SELECT id FROM serial.distribution WHERE holding_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM serial.distribution_note WHERE distribution IN 
(SELECT id FROM serial.distribution WHERE holding_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM serial.index_summary WHERE distribution IN 
(SELECT id FROM serial.distribution WHERE holding_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM serial.item WHERE stream IN 
(SELECT id FROM serial.stream WHERE distribution IN 
(SELECT id FROM serial.distribution WHERE holding_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)));

DELETE FROM serial.item_note WHERE id IN 
(SELECT id FROM serial.item WHERE stream IN (SELECT id FROM serial.stream WHERE distribution IN 
(SELECT id FROM serial.distribution WHERE holding_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del))));

DELETE FROM serial.stream WHERE distribution IN 
(SELECT id FROM serial.distribution WHERE holding_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM serial.supplement_summary WHERE distribution IN 
(SELECT id FROM serial.distribution WHERE holding_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM serial.distribution WHERE holding_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

UPDATE serial.record_entry SET editor = 1 WHERE editor IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

UPDATE serial.record_entry SET creator = 1 WHERE creator IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

UPDATE serial.record_entry SET owning_lib = 1 
WHERE owning_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
AND id IN (SELECT record_entry FROM serial.distribution WHERE holding_lib NOT IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM serial.record_entry WHERE owning_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

COMMIT;

ALTER TABLE serial.record_entry ENABLE RULE protect_mfhd_delete;
