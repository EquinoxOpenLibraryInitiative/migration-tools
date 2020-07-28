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

-- we often see edge cases of transactions that survive because they belong to other 
-- systems in consortiums but used materials by the library being remmoved

BEGIN;

INSERT INTO asset.copy (circ_lib,creator,editor,call_number,loan_duration,fine_level,barcode) VALUES (1,1,1,-1,2,2,CONCAT_WS('_','precat_for_deleted_org',:ou_to_del));

UPDATE action.circulation SET target_copy = (SELECT id FROM asset.copy WHERE barcode = CONCAT_WS('_','precat_for_deleted_org',:ou_to_del)) 
	WHERE id IN (SELECT acirc.id FROM action.circulation acirc LEFT JOIN asset.copy acp ON acp.id = acirc.target_copy WHERE acp.id IS NULL);

UPDATE action.hold_request SET target = (SELECT id FROM asset.copy WHERE barcode = CONCAT_WS('_','precat_for_deleted_org',:ou_to_del)) 
	WHERE id IN (SELECT ahr.id FROM action.hold_request ahr LEFT JOIN asset.copy acp ON acp.id = ahr.target WHERE acp.id IS NULL AND ahr.hold_type = 'C');

UPDATE action.hold_request SET target = -1 WHERE id IN 
	(SELECT ahr.id FROM action.hold_request ahr LEFT JOIN asset.call_number acn ON acn.id = ahr.target WHERE acn.id IS NULL AND ahr.hold_type = 'V');

UPDATE action.hold_request SET target = -1 WHERE id IN 
    (SELECT ahr.id FROM action.hold_request ahr LEFT JOIN biblio.record_entry bre ON bre.id = ahr.target WHERE bre.id IS NULL AND ahr.hold_type = 'T');

UPDATE action.hold_request SET current_copy = (SELECT id FROM asset.copy WHERE barcode = CONCAT_WS('_','precat_for_deleted_org',:ou_to_del))  
	WHERE id IN (SELECT ahr.id FROM action.hold_request ahr LEFT JOIN asset.copy acp ON acp.id = ahr.target WHERE acp.id IS NULL AND ahr.current_copy IS NOT NULL);
-- problem can be duplicates ... ug 

-- delete instead of update here because it's not statistical and not useful if pointed to a pre-cat 
DELETE FROM action.usr_circ_history WHERE id IN 
	(SELECT uch.id FROM action.usr_circ_history uch LEFT JOIN asset.copy acp ON acp.id = uch.target_copy WHERE acp.id IS NULL);

UPDATE action.aged_circulation SET target_copy = (SELECT id FROM asset.copy WHERE barcode = CONCAT_WS('_','precat_for_deleted_org',:ou_to_del))
	WHERE id IN (SELECT aac.id FROM action.aged_circulation aac LEFT JOIN asset.copy acp ON acp.id = aac.target_copy WHERE acp.id IS NULL);

UPDATE action.aged_hold_request SET current_copy = (SELECT id FROM asset.copy WHERE barcode = CONCAT_WS('_','precat_for_deleted_org',:ou_to_del)) 
	WHERE id IN (SELECT aahr.id FROM action.aged_hold_request aahr LEFT JOIN asset.copy acp ON acp.id = aahr.current_copy WHERE acp.id IS NULL);


UPDATE action.aged_hold_request SET target = (SELECT id FROM asset.copy WHERE barcode = CONCAT_WS('_','precat_for_deleted_org',:ou_to_del))
    WHERE id IN (SELECT aahr.id FROM action.aged_hold_request aahr LEFT JOIN asset.copy acp ON acp.id = aahr.target WHERE acp.id IS NULL AND aahr.hold_type = 'C');

UPDATE action.aged_hold_request SET target = -1 WHERE id IN
    (SELECT aahr.id FROM action.aged_hold_request aahr  LEFT JOIN asset.call_number acn ON acn.id = aahr.target WHERE acn.id IS NULL AND aahr.hold_type = 'V');

UPDATE action.aged_hold_request SET target = -1 WHERE id IN
    (SELECT aahr.id FROM action.aged_hold_request aahr  LEFT JOIN biblio.record_entry bre ON bre.id = aahr.target WHERE bre.id IS NULL AND aahr.hold_type = 'T');

COMMIT;

 
