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
\set vol_del_table ORGUNIT_volume_bibs
\set ECHO all
\timing

ALTER TABLE biblio.monograph_part DISABLE RULE protect_mono_part_delete;

BEGIN;

UPDATE biblio.record_entry SET merge_date = NULL, merged_to = NULL WHERE merged_to IN
(
    SELECT record FROM esi.:vol_del_table x
    WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
);

DELETE FROM authority.bib_linking WHERE bib IN 
(
    SELECT record FROM esi.:vol_del_table x
    WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
);

DELETE FROM metabib.browse_entry_def_map WHERE source IN 
(
    SELECT record FROM esi.:vol_del_table x
    WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
);

DELETE FROM acq.lineitem WHERE eg_bib_id IN 
(
    SELECT record FROM esi.:vol_del_table x
    WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
);

DELETE FROM biblio.monograph_part WHERE record IN 
(
    SELECT record FROM esi.:vol_del_table x
    WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
);

DELETE FROM biblio.peer_bib_copy_map WHERE peer_record IN 
(
    SELECT record FROM esi.:vol_del_table x
    WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
);

DELETE FROM acq.lineitem WHERE queued_record IN (
    SELECT id FROM vandelay.queued_bib_record WHERE imported_as IN
    (
        SELECT record FROM esi.:vol_del_table x
        WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
    )
);

DELETE FROM acq.acq_lineitem_history WHERE queued_record IN (
    SELECT id FROM vandelay.queued_bib_record WHERE imported_as IN
    (
        SELECT record FROM esi.:vol_del_table x
        WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
    )
);

DELETE FROM vandelay.queued_bib_record WHERE imported_as IN 
(
    SELECT record FROM esi.:vol_del_table x
    WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
);

DELETE FROM metabib.record_attr_vector_list WHERE source IN 
(
    SELECT record FROM esi.:vol_del_table x
    WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
);

DELETE FROM metabib.record_sorter WHERE source IN 
(
    SELECT record FROM esi.:vol_del_table x
    WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
);

DELETE FROM booking.resource_type WHERE record IN 
(
    SELECT record FROM esi.:vol_del_table x
    WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
);

DELETE FROM metabib.display_entry WHERE source IN
(
    SELECT record FROM esi.:vol_del_table x
    WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
);

DELETE FROM metabib.real_full_rec WHERE record IN
(
    SELECT record FROM esi.:vol_del_table x
    WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
);

COMMIT;


ALTER TABLE biblio.monograph_part ENABLE RULE protect_mono_part_delete;
