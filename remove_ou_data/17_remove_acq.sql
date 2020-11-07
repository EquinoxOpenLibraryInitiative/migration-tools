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

DELETE FROM acq.cancel_reason WHERE org_unit IN (
    SELECT (actor.org_unit_descendants(id)).id FROM actor.org_unit WHERE shortname = :ou_to_del
);

DELETE FROM acq.lineitem WHERE picklist IN (SELECT id FROM acq.picklist WHERE org_unit IN 
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del)
);

DELETE FROM acq.funding_source_credit WHERE funding_source IN (
    SELECT id FROM acq.funding_source WHERE owner IN (
        SELECT (actor.org_unit_descendants(id)).id FROM actor.org_unit WHERE shortname = :ou_to_del)
);

DELETE FROM acq.funding_source WHERE owner IN (
    SELECT (actor.org_unit_descendants(id)).id FROM actor.org_unit WHERE shortname = :ou_to_del
);

DELETE FROM acq.picklist WHERE org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del); 

DELETE FROM acq.lineitem WHERE creator IN (
    SELECT id FROM actor.usr WHERE home_ou IN (
        SELECT (actor.org_unit_descendants(id)).id FROM actor.org_unit WHERE shortname = :ou_to_del
    ) 
);

DELETE FROM acq.lineitem WHERE editor IN (
    SELECT id FROM actor.usr WHERE home_ou IN (
        SELECT (actor.org_unit_descendants(id)).id FROM actor.org_unit WHERE shortname = :ou_to_del
    ) 
);

DELETE FROM acq.po_note WHERE creator IN (
    SELECT id FROM actor.usr WHERE home_ou IN (
        SELECT (actor.org_unit_descendants(id)).id FROM actor.org_unit WHERE shortname = :ou_to_del
    ) 
);

DELETE FROM acq.po_note WHERE editor IN (
    SELECT id FROM actor.usr WHERE home_ou IN (
        SELECT (actor.org_unit_descendants(id)).id FROM actor.org_unit WHERE shortname = :ou_to_del
    )
);


DELETE FROM acq.purchase_order WHERE owner IN ( 
    SELECT id FROM actor.usr WHERE home_ou IN (
        SELECT (actor.org_unit_descendants(id)).id FROM actor.org_unit WHERE shortname = :ou_to_del
    ) 
);

DELETE FROM acq.user_request WHERE usr IN  (
    SELECT id FROM actor.usr WHERE home_ou IN (
        SELECT (actor.org_unit_descendants(id)).id FROM actor.org_unit WHERE shortname = :ou_to_del
    )
);

DELETE FROM acq.fund_allocation WHERE allocator IN (
    SELECT id FROM actor.usr WHERE home_ou IN (
        SELECT (actor.org_unit_descendants(id)).id FROM actor.org_unit WHERE shortname = :ou_to_del
    )
);

DELETE FROM acq.fund_transfer WHERE transfer_user IN (
    SELECT id FROM actor.usr WHERE home_ou IN (
        SELECT (actor.org_unit_descendants(id)).id FROM actor.org_unit WHERE shortname = :ou_to_del
    )
);

DELETE FROM acq.invoice WHERE closed_by IN (
    SELECT id FROM actor.usr WHERE home_ou IN (
        SELECT (actor.org_unit_descendants(id)).id FROM actor.org_unit WHERE shortname = :ou_to_del
    )
);

DELETE FROM acq.invoice WHERE receiver IN (
    SELECT (actor.org_unit_descendants(id)).id FROM actor.org_unit WHERE shortname = :ou_to_del
);

DELETE FROM acq.purchase_order WHERE editor IN (
    SELECT id FROM actor.usr WHERE home_ou IN (
        SELECT (actor.org_unit_descendants(id)).id FROM actor.org_unit WHERE shortname = :ou_to_del
    )
);

COMMIT;
