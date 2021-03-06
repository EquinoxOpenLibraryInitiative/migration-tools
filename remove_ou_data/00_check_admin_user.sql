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

-- these scripts assume that the admin user's home_ou is 1, if not there can be problems 
-- replace 'equinox' with the appropriate admin name 

DO $$
DECLARE 
    x   INTEGER;
BEGIN
    SELECT home_ou FROM actor.usr WHERE usrname = 'equinox' INTO x;
    IF x != 1 THEN RAISE EXCEPTION 'Admin user is not home org of 1'; END IF;
END $$;
