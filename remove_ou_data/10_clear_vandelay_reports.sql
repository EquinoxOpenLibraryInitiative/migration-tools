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

INSERT INTO reporter.template_folder (owner,name) values (1,'saved_cons_templates');
INSERT INTO reporter.output_folder (owner,name) values (1,'saved_cons_output');

BEGIN;

DELETE FROM vandelay.queue WHERE owner IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM reporter.report WHERE owner IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM reporter.report_folder WHERE owner IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM reporter.report WHERE template IN 
    (SELECT id FROM reporter.template WHERE owner IN
        (SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del))
    ) AND id NOT IN (SELECT report FROM reporter.schedule WHERE complete_time IS NULL);

DELETE FROM reporter.output_folder WHERE owner IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

UPDATE reporter.template a SET owner = 1, folder = (SELECT id FROM reporter.template_folder WHERE name ~* 'saved_cons_templates' and owner = 1) 
FROM (SELECT id, template FROM reporter.report WHERE owner = 1) x 
WHERE x.template = a.id AND a.owner IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

UPDATE reporter.schedule SET folder = (SELECT id FROM reporter.output_folder WHERE owner = 1 AND name = 'saved_cons_output') WHERE folder IN
(SELECT id FROM reporter.output_folder WHERE share_with IN 
    (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM reporter.output_folder WHERE share_with IN
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

DELETE FROM reporter.schedule WHERE runner IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM reporter.template WHERE owner IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

DELETE FROM reporter.template_folder WHERE owner IN
(SELECT id FROM actor.usr WHERE home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));

UPDATE reporter.report_folder SET share_with = NULL WHERE share_with IN 
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

UPDATE reporter.template_folder SET share_with = NULL WHERE share_with IN 
(SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);

COMMIT;
