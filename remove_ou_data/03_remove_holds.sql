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

ALTER TABLE action.hold_request DISABLE TRIGGER action_hold_request_aging_tgr;

BEGIN;

DELETE FROM action.hold_request WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM action.hold_request WHERE current_shelf_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM action.hold_request WHERE pickup_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM action.hold_request WHERE request_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

DELETE FROM action.aged_hold_request WHERE pickup_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

DELETE FROM action.aged_hold_request WHERE request_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

-- FIXME: hold placed by EXAMPLE staff for non-EXAMPLE patron
DELETE FROM action.hold_request WHERE requestor IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM action.hold_request WHERE fulfillment_time IS NOT NULL AND fulfillment_lib IN 
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

COMMIT;

ALTER TABLE action.hold_request ENABLE TRIGGER action_hold_request_aging_tgr;
