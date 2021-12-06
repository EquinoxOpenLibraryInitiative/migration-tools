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

CREATE INDEX tmp_ws_indx1 ON action.circulation (workstation);
CREATE INDEX tmp_ws_indx2 ON action.circulation (checkin_workstation);
CREATE INDEX tmp_ws_indx3 ON money.bnm_desk_payment(cash_drawer);
ALTER TABLE action.circulation DISABLE TRIGGER action_circulation_target_copy_trig;

BEGIN;

UPDATE action.circulation SET checkin_lib = NULL, checkin_workstation = NULL
WHERE checkin_lib IN
(SELECT (actor.org_unit_descendants(id)).id from
 actor.org_unit where shortname = :ou_to_del);

UPDATE asset.latest_inventory SET inventory_workstation = NULL 
WHERE inventory_workstation IN (
    SELECT id FROM actor.workstation WHERE owning_lib IN
    (
        SELECT (actor.org_unit_descendants(id)).id from
        actor.org_unit where shortname = 'YCYTS')
    )   
); 

DELETE FROM actor.workstation WHERE owning_lib IN
(SELECT (actor.org_unit_descendants(id)).id from
 actor.org_unit where shortname = :ou_to_del);

COMMIT;

DROP INDEX action.tmp_ws_indx1;
DROP INDEX action.tmp_ws_indx2;
DROP INDEX money.tmp_ws_indx3;
ALTER TABLE action.circulation ENABLE TRIGGER action_circulation_target_copy_trig;
