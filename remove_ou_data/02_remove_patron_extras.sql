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

ALTER TABLE actor.usr_address DISABLE TRIGGER audit_actor_usr_address_update_trigger;
ALTER TABLE actor.usr DISABLE TRIGGER audit_actor_usr_update_trigger;
ALTER TABLE actor.usr_message DISABLE RULE protect_usr_message_delete;

CREATE INDEX tmp_addr_replaces ON actor.usr_address(replaces);
BEGIN;

DELETE FROM actor.usr_note WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM actor.stat_cat_entry_usr_map WHERE target_usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM actor.usr_activity WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM actor.usr_org_unit_opt_in WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM actor.usr_password_reset WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM actor.usr_saved_search WHERE owner IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM actor.usr_setting WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM actor.usr_standing_penalty WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM actor.card WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
UPDATE actor.usr SET card = NULL
WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

UPDATE actor.usr SET mailing_address = NULL, billing_address = NULL
WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.usr_address WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM actor.usr_message WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM actor.usr_message WHERE sending_lib IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

DELETE FROM actor.stat_cat_entry_usr_map WHERE target_usr IN 
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM actor.stat_cat_entry_usr_map WHERE stat_cat IN 
(SELECT id from actor.stat_cat WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM actor.stat_cat_entry WHERE stat_cat IN 
(SELECT id from actor.stat_cat WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM actor.stat_cat WHERE id IN 
(SELECT id from actor.stat_cat WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM action.survey_response WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM action.survey_question WHERE survey IN 
(SELECT id FROM action.survey WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM action.survey WHERE owner IN 
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

DELETE FROM permission.usr_perm_map WHERE usr IN 
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM permission.usr_grp_map WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

COMMIT;

DROP INDEX actor.tmp_addr_replaces;
ALTER TABLE actor.usr_address ENABLE TRIGGER audit_actor_usr_address_update_trigger;
ALTER TABLE actor.usr ENABLE TRIGGER audit_actor_usr_update_trigger;
ALTER TABLE actor.usr_message ENABLE RULE protect_usr_message_delete;
