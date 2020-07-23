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

BEGIN;

DELETE FROM action.non_cataloged_circulation WHERE patron IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM action.non_cataloged_circulation WHERE staff IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM action.usr_circ_history WHERE usr IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM acq.user_request WHERE usr IN 
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM acq.lineitem WHERE creator IN 
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM acq.lineitem WHERE selector IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM acq.lineitem WHERE editor IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM acq.lineitem WHERE purchase_order IN 
(SELECT id FROM acq.purchase_order WHERE creator IN 
	(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)));
DELETE FROM acq.po_note WHERE purchase_order IN 
(SELECT id FROM acq.purchase_order WHERE creator IN
    (SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)));
DELETE FROM acq.purchase_order WHERE creator IN 
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM acq.purchase_order WHERE owner IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM acq.fund_allocation WHERE fund IN 
(SELECT id FROM acq.fund WHERE org IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM acq.invoice_item WHERE fund_debit IN 
(SELECT id FROM acq.fund_debit WHERE fund IN (SELECT id FROM acq.fund WHERE org IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)));
DELETE FROM acq.invoice WHERE receiver IN 
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM acq.lineitem_detail WHERE fund IN 
(SELECT id FROM acq.fund WHERE org IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)); 
DELETE FROM acq.fund_debit WHERE fund IN 
(SELECT id FROM acq.fund WHERE org IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM acq.fund_transfer WHERE src_fund IN 
(SELECT id FROM acq.fund WHERE org IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM acq.fund_transfer WHERE dest_fund IN 
(SELECT id FROM acq.fund WHERE org IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM acq.fund WHERE org IN  (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);


COMMIT;
