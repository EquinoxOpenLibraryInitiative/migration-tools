-- Copyright 2009-2013, Equinox Software, Inc.
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

ALTER TABLE actor.usr DISABLE RULE protect_user_delete;
BEGIN;

-- get transactions out of the way first
TRUNCATE TABLE action.circulation CASCADE;
TRUNCATE TABLE action.hold_request CASCADE;
TRUNCATE TABLE money.payment CASCADE;
TRUNCATE TABLE money.billing CASCADE;
TRUNCATE TABLE money.grocery CASCADE;
TRUNCATE TABLE money.materialized_billable_xact_summary CASCADE;
TRUNCATE TABLE action.non_cataloged_circulation CASCADE;
TRUNCATE TABLE action.in_house_use CASCADE;

-- This statement is meant to be customized
DELETE FROM actor.usr WHERE usrname !~ 'admin' 
AND profile NOT IN (SELECT id FROM permission.grp_tree WHERE name IN ('SIP', 'Unique Mgmt'));

\echo List of patrons that are left
SELECT id, usrname FROM actor.usr;

DELETE FROM actor.usr_note WHERE usr NOT IN (SELECT id FROM actor.usr);
DELETE FROM actor.usr_address WHERE usr NOT IN (SELECT id FROM actor.usr);
DELETE FROM actor.card WHERE usr NOT IN (SELECT id FROM actor.usr);
DELETE FROM money.collections_tracker WHERE usr NOT IN (SELECT id FROM actor.usr);
DELETE FROM reporter.template_folder WHERE owner NOT IN (SELECT id FROM actor.usr);
DELETE FROM reporter.report_folder WHERE owner NOT IN (SELECT id FROM actor.usr);
DELETE FROM reporter.output_folder WHERE owner NOT IN (SELECT id FROM actor.usr);
DELETE FROM reporter.template WHERE owner NOT IN (SELECT id FROM actor.usr);
DELETE FROM reporter.report WHERE owner NOT IN (SELECT id FROM actor.usr);
DELETE FROM reporter.schedule WHERE runner NOT IN (SELECT id FROM actor.usr);

\echo If you are happy with the purge, please run the following:
\echo
\echo COMMIT;
\echo ALTER TABLE actor.usr ENABLE RULE protect_user_delete;
\echo
\echo Finally, please do a VACUUM ANALYZE
