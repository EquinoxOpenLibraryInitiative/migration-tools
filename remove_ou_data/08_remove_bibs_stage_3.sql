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

ALTER TABLE biblio.record_entry DISABLE TRIGGER audit_biblio_record_entry_update_trigger;
ALTER TABLE biblio.record_entry DISABLE TRIGGER bbb_simple_rec_trigger;
ALTER TABLE biblio.record_entry DISABLE RULE protect_bib_rec_delete;
CREATE INDEX tmp_bib_idx1 ON acq.lineitem (eg_bib_id);

BEGIN;

DELETE FROM biblio.record_entry WHERE id IN (
    SELECT record FROM esi.albemarle_volume_bibs x 
    WHERE NOT EXISTS (select 1 from asset.call_number where record = x.record)
);

COMMIT;

ALTER TABLE biblio.record_entry ENABLE RULE protect_bib_rec_delete;
ALTER TABLE biblio.record_entry ENABLE TRIGGER bbb_simple_rec_trigger;
ALTER TABLE biblio.record_entry ENABLE TRIGGER audit_biblio_record_entry_update_trigger;
DROP INDEX acq.tmp_bib_idx1;
