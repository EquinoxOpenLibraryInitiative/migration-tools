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

DELETE FROM acq.distribution_formula_entry WHERE owning_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM acq.fund_allocation_percent WHERE org IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM action.fieldset WHERE owning_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM action.in_house_use WHERE org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM action.non_cataloged_circulation WHERE circ_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM action.non_cat_in_house_use WHERE org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.address_alert WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.hours_of_operation WHERE id IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.org_lasso_map WHERE org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.org_unit_closed WHERE org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.org_unit_custom_tree_node WHERE org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.org_unit_proximity_adjustment WHERE hold_pickup_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.org_unit_proximity_adjustment WHERE hold_request_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.org_unit_proximity_adjustment WHERE item_circ_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.org_unit_proximity_adjustment WHERE item_owning_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.org_unit_setting WHERE org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.search_filter_group WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.stat_cat_entry_default WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.stat_cat_entry WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.toolbar WHERE org IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.usr_org_unit_opt_in WHERE org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM actor.usr_standing_penalty WHERE org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM serial.distribution WHERE holding_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM serial.record_entry WHERE owning_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM serial.subscription WHERE owning_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM asset.copy_template WHERE circ_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM asset.copy_template WHERE owning_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM booking.reservation WHERE pickup_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM booking.reservation WHERE request_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM booking.resource_attr WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM booking.resource_attr_value WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM booking.resource WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM booking.resource_type WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.barcode_completion WHERE org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.circ_limit_set WHERE owning_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.circ_matrix_matchpoint WHERE copy_circ_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.circ_matrix_matchpoint WHERE copy_owning_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.circ_matrix_matchpoint WHERE org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.circ_matrix_matchpoint WHERE user_home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.filter_dialog_filter_set WHERE owning_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.floating_group_member WHERE org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.hold_matrix_matchpoint WHERE item_circ_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.hold_matrix_matchpoint WHERE item_owning_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.hold_matrix_matchpoint WHERE pickup_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.hold_matrix_matchpoint WHERE request_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.hold_matrix_matchpoint WHERE user_home_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.idl_field_doc WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.org_unit_setting_type_log WHERE org IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.remote_account WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.weight_assoc WHERE org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.z3950_source_credentials WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM money.collections_tracker WHERE location IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM permission.grp_penalty_threshold WHERE org_unit IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM permission.usr_work_ou_map WHERE work_ou IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM url_verify.session WHERE owning_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM vandelay.import_bib_trash_group WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM vandelay.import_item_attr_definition WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM vandelay.match_set WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM vandelay.merge_profile WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM asset.copy_tag_copy_map WHERE copy IN (SELECT id FROM asset.copy WHERE circ_lib IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del));
DELETE FROM asset.copy_tag WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
DELETE FROM config.copy_tag_type WHERE owner IN (SELECT (actor.org_unit_descendants(id)).id from actor.org_unit where shortname = :ou_to_del);
COMMIT;
