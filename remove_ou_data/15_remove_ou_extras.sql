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

create index tmp_idx on money.billing(btype);
ALTER TABLE action.circulation DISABLE TRIGGER action_circulation_target_copy_trig;
ALTER TABLE asset.copy_location DISABLE RULE protect_copy_location_delete;

BEGIN;

DELETE FROM actor.org_address WHERE org_unit IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

UPDATE actor.org_unit SET ill_address = NULL, holds_address = NULL, mailing_address = NULL, billing_address = NULL
WHERE id IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

UPDATE action.circulation SET copy_location = 1
WHERE copy_location IN (SELECT id FROM asset.copy_location WHERE owning_lib IN 
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM asset.copy_location WHERE owning_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

DELETE FROM money.account_adjustment WHERE billing IN 
    (SELECT id FROM money.billing WHERE btype IN (
        SELECT id FROM config.billing_type
        WHERE owner IN
        (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
));

DELETE FROM money.billing
WHERE btype IN (
    SELECT id FROM config.billing_type
    WHERE owner IN
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
);

DROP TABLE IF EXISTS esi.temp_action_trigger_output_list;
CREATE UNLOGGED TABLE esi.temp_action_trigger_output_list AS
SELECT template_output AS output_id FROM action_trigger.event
WHERE  event_def IN (
    SELECT id FROM action_trigger.event_definition
    WHERE owner IN
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
)
UNION ALL
SELECT error_output AS output_id FROM action_trigger.event
WHERE  event_def IN (
    SELECT id FROM action_trigger.event_definition
    WHERE owner IN
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
)
UNION ALL
SELECT async_output AS output_id FROM action_trigger.event
WHERE  event_def IN (
    SELECT id FROM action_trigger.event_definition
    WHERE owner IN
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
);

DELETE FROM action_trigger.event
WHERE event_def IN (
    SELECT id FROM action_trigger.event_definition
    WHERE owner IN
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
);  
DELETE FROM action_trigger.event_output WHERE id IN (
    SELECT output_id FROM esi.temp_action_trigger_output_list
);
DELETE FROM action_trigger.environment
WHERE event_def IN (
    SELECT id FROM action_trigger.event_definition
    WHERE owner IN
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
);
DELETE FROM action_trigger.event_params
WHERE event_def IN (
    SELECT id FROM action_trigger.event_definition
    WHERE owner IN
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
);
DELETE FROM action_trigger.event_definition
    WHERE owner IN
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);


COMMIT;
DROP TABLE esi.temp_action_trigger_output_list;
DROP INDEX money.tmp_idx;
ALTER TABLE asset.copy_location ENABLE RULE protect_copy_location_delete;
ALTER TABLE action.circulation ENABLE TRIGGER action_circulation_target_copy_trig;
