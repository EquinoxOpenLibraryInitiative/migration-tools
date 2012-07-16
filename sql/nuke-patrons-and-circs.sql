-- Copyright 2009-2012, Equinox Software, Inc.
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

alter table actor.usr disable rule protect_user_delete;
begin;
delete from actor.usr where usrname !~ 'admin';
delete from actor.usr_note where usr not in (select id from actor.usr);
delete from actor.usr_address where usr not in (select id from actor.usr);
delete from actor.card where usr not in (select id from actor.usr);
delete from action.circulation;
delete from action.hold_request;
delete from money.billing;
delete from money.grocery;
delete from action.non_cataloged_circulation;
delete from action.in_house_use;
delete from reporter.template_folder where owner not in (select id from actor.usr);
delete from reporter.report_folder where owner not in (select id from actor.usr);
delete from reporter.output_folder where owner not in (select id from actor.usr);
delete from reporter.template where owner not in (select id from actor.usr);
delete from reporter.report where owner not in (select id from actor.usr);
delete from reporter.schedule where runner not in (select id from actor.usr);
commit;
alter table actor.usr enable rule protect_user_delete;
