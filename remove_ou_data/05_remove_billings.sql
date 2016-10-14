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

ALTER TABLE money.cash_payment DISABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.goods_payment DISABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.credit_payment DISABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.check_payment DISABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.credit_card_payment DISABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.forgive_payment DISABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.payment DISABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.billing DISABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.grocery DISABLE TRIGGER mat_summary_remove_tgr;

BEGIN;

DELETE FROM money.credit_payment
WHERE xact IN (
SELECT usr FROM money.billable_xact WHERE usr IN 
    (SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del))
);

DELETE FROM money.credit_card_payment
WHERE xact IN (
SELECT usr FROM money.billable_xact WHERE usr IN 
    (SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del))
);

DELETE FROM money.check_payment
WHERE xact IN (
SELECT usr FROM money.billable_xact WHERE usr IN 
    (SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del))
);

DELETE FROM money.cash_payment
WHERE xact IN (
SELECT usr FROM money.billable_xact WHERE usr IN 
    (SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del))
);

DELETE FROM money.goods_payment
WHERE xact IN (
SELECT usr FROM money.billable_xact WHERE usr IN 
    (SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del))
);

DELETE FROM money.work_payment
WHERE xact IN (
SELECT usr FROM money.billable_xact WHERE usr IN 
    (SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del))
);

DELETE FROM money.forgive_payment
WHERE xact IN (
SELECT usr FROM money.billable_xact WHERE usr IN 
    (SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del))
);

DELETE FROM money.bnm_desk_payment
WHERE xact IN (
SELECT usr FROM money.billable_xact WHERE usr IN 
    (SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del))
);

DELETE FROM money.payment
WHERE xact IN (
SELECT usr FROM money.billable_xact WHERE usr IN 
    (SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del))
);

DELETE FROM money.billing
WHERE xact IN (
SELECT usr FROM money.billable_xact WHERE usr IN 
    (SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del))
);

DELETE FROM money.grocery WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM money.billable_xact WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM money.materialized_billable_xact_summary WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
COMMIT;

ALTER TABLE money.cash_payment ENABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.goods_payment ENABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.credit_payment ENABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.check_payment ENABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.credit_card_payment ENABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.forgive_payment ENABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.payment ENABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.billing ENABLE TRIGGER mat_summary_del_tgr;
ALTER TABLE money.grocery ENABLE TRIGGER mat_summary_remove_tgr;
