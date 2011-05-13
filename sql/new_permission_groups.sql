--
-- Run this script immediately after a normal installation
-- (assuming LP bug 782268 is rejected) in order to implement
-- the new permission groups.
--

-- Alter the permission hierarchy

UPDATE permission.grp_tree SET description = oils_i18n_gettext(10, 'Can do anything at the Branch level', 'pgt', 'description') WHERE id = 10;

INSERT INTO permission.grp_tree (id, name, parent, description, perm_interval, usergroup, application_perm) VALUES
	(8, oils_i18n_gettext(8, 'Cataloging Administrator', 'pgt', 'name'), 3, NULL, '3 years', TRUE, 'group_application.user.staff.cat_admin');
INSERT INTO permission.grp_tree (id, name, parent, description, perm_interval, usergroup, application_perm) VALUES
	(9, oils_i18n_gettext(9, 'Circulation Administrator', 'pgt', 'name'), 3, NULL, '3 years', TRUE, 'group_application.user.staff.circ_admin');
INSERT INTO permission.grp_tree (id, name, parent, description, perm_interval, usergroup, application_perm) VALUES
	(11, oils_i18n_gettext(11, 'Serials', 'pgt', 'name'), 3, 
	oils_i18n_gettext(11, 'Serials (includes admin features)', 'pgt', 'description'), '3 years', TRUE, 'group_application.user.staff.serials');
INSERT INTO permission.grp_tree (id, name, parent, description, perm_interval, usergroup, application_perm) VALUES
	(12, oils_i18n_gettext(12, 'System Administrator', 'pgt', 'name'), 3, 
	oils_i18n_gettext(12, 'Can do anything at the System level', 'pgt', 'description'), '3 years', TRUE, 'group_application.user.staff.admin.system_admin');
INSERT INTO permission.grp_tree (id, name, parent, description, perm_interval, usergroup, application_perm) VALUES
	(13, oils_i18n_gettext(13, 'Global Administrator', 'pgt', 'name'), 3, 
	oils_i18n_gettext(13, 'Can do anything at the Consortium level', 'pgt', 'description'), '3 years', TRUE, 'group_application.user.staff.admin.global_admin');
INSERT INTO permission.grp_tree (id, name, parent, description, perm_interval, usergroup, application_perm) VALUES
	(14, oils_i18n_gettext(14, 'Data Review', 'pgt', 'name'), 3, NULL, '3 years', TRUE, 'group_application.user.staff.data_review');
INSERT INTO permission.grp_tree (id, name, parent, description, perm_interval, usergroup, application_perm) VALUES
	(15, oils_i18n_gettext(15, 'Volunteers', 'pgt', 'name'), 3, NULL, '3 years', TRUE, 'group_application.user.staff.volunteers');

SELECT SETVAL('permission.grp_tree_id_seq'::TEXT, (SELECT MAX(id) FROM permission.grp_tree));


-- Wipe out existing permissions

DELETE FROM permission.usr_grp_map WHERE usr <> 1;


-- Add basic user permissions to the Users group

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Users' AND
		aout.name = 'Consortium' AND
		perm.code IN (
			'COPY_CHECKIN',
			'CREATE_MY_CONTAINER',
			'MR_HOLDS',
			'OPAC_LOGIN',
			'RENEW_CIRC',
			'TITLE_HOLDS',
			'user_request.create');


-- Add basic user permissions to the Data Review group

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Data Review' AND
		aout.name = 'Consortium' AND
		perm.code IN (
			'CREATE_COPY_TRANSIT',
			'VIEW_BILLING_TYPE',
			'VIEW_CIRCULATIONS',
			'VIEW_COPY_NOTES',
			'VIEW_HOLD',
			'VIEW_ORG_SETTINGS',
			'VIEW_TITLE_NOTES',
			'VIEW_TRANSACTION',
			'VIEW_USER',
			'VIEW_USER_FINES_SUMMARY',
			'VIEW_USER_TRANSACTIONS',
			'VIEW_VOLUME_NOTES',
			'VIEW_ZIP_DATA');

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Data Review' AND
		aout.name = 'System' AND
		perm.code IN (
			'COPY_CHECKOUT',
			'COPY_HOLDS',
			'CREATE_IN_HOUSE_USE',
			'CREATE_TRANSACTION',
			'OFFLINE_EXECUTE',
			'OFFLINE_VIEW',
			'STAFF_LOGIN',
			'VOLUME_HOLDS');


-- Add basic staff permissions to the Staff group

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Staff' AND
		aout.name = 'Consortium' AND
		perm.code IN (
			'CREATE_CONTAINER',
			'CREATE_CONTAINER_ITEM',
			'CREATE_COPY_TRANSIT',
			'CREATE_HOLD_NOTIFICATION',
			'CREATE_TRANSACTION',
			'CREATE_TRANSIT',
			'DELETE_CONTAINER',
			'DELETE_CONTAINER_ITEM',
			'group_application.user',
			'group_application.user.patron',
			'REGISTER_WORKSTATION',
			'REMOTE_Z3950_QUERY',
			'REQUEST_HOLDS',
			'STAFF_LOGIN',
			'TRANSIT_COPY',
			'UPDATE_CONTAINER',
			'VIEW_CONTAINER',
			'VIEW_COPY_CHECKOUT_HISTORY',
			'VIEW_COPY_NOTES',
			'VIEW_HOLD',
			'VIEW_HOLD_NOTIFICATION',
			'VIEW_HOLD_PERMIT',
			'VIEW_PERM_GROUPS',
			'VIEW_PERMISSION',
			'VIEW_TITLE_NOTES',
			'VIEW_TRANSACTION',
			'VIEW_VOLUME_NOTES');

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Staff' AND
		aout.name = 'System' AND
		perm.code IN (
			'CREATE_USER',
			'UPDATE_USER',
			'VIEW_BILLING_TYPE',
			'VIEW_CIRCULATIONS',
			'VIEW_ORG_SETTINGS',
			'VIEW_PERMIT_CHECKOUT',
			'VIEW_USER',
			'VIEW_USER_FINES_SUMMARY',
			'VIEW_USER_TRANSACTIONS');

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Staff' AND
		aout.name = 'Branch' AND
		perm.code IN (
			'CANCEL_HOLDS',
			'COPY_CHECKOUT',
			'COPY_HOLDS',
			'COPY_TRANSIT_RECEIVE',
			'CREATE_BILL',
			'CREATE_IN_HOUSE_USE',
			'CREATE_PAYMENT',
			'RENEW_HOLD_OVERRIDE',
			'UPDATE_COPY',
			'UPDATE_VOLUME',
			'VOLUME_HOLDS');


-- Add basic cataloguing permissions to the Catalogers group

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Catalogers' AND
		aout.name = 'Consortium' AND
		perm.code IN (
			'ALLOW_ALT_TCN',
			'CREATE_BIB_IMPORT_QUEUE',
			'CREATE_IMPORT_ITEM',
			'CREATE_MARC',
			'CREATE_TITLE_NOTE',
			'DELETE_BIB_IMPORT_QUEUE',
			'DELETE_IMPORT_ITEM',
			'DELETE_RECORD',
			'DELETE_TITLE_NOTE',
			'IMPORT_ACQ_LINEITEM_BIB_RECORD',
			'IMPORT_MARC',
			'MERGE_AUTH_RECORDS',
			'MERGE_BIB_RECORDS',
			'UPDATE_AUTHORITY_IMPORT_QUEUE',
			'UPDATE_AUTHORITY_RECORD_NOTE',
			'UPDATE_BIB_IMPORT_QUEUE',
			'UPDATE_MARC',
			'UPDATE_RECORD',
			'user_request.view',
			'VIEW_AUTHORITY_RECORD_NOTES');

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Catalogers' AND
		aout.name = 'System' AND
		perm.code IN (
			'CREATE_COPY',
			'CREATE_COPY_NOTE',
			'CREATE_MFHD_RECORD',
			'CREATE_VOLUME',
			'CREATE_VOLUME_NOTE',
			'DELETE_COPY',
			'DELETE_COPY_NOTE',
			'DELETE_MFHD_RECORD',
			'DELETE_VOLUME',
			'DELETE_VOLUME_NOTE',
			'MARK_ITEM_AVAILABLE',
			'MARK_ITEM_BINDERY',
			'MARK_ITEM_CHECKED_OUT',
			'MARK_ITEM_ILL',
			'MARK_ITEM_IN_PROCESS',
			'MARK_ITEM_IN_TRANSIT',
			'MARK_ITEM_LOST',
			'MARK_ITEM_MISSING',
			'MARK_ITEM_ON_HOLDS_SHELF',
			'MARK_ITEM_ON_ORDER',
			'MARK_ITEM_RESHELVING',
			'UPDATE_COPY',
			'UPDATE_COPY_NOTE',
			'UPDATE_IMPORT_ITEM',
			'UPDATE_MFHD_RECORD',
			'UPDATE_VOLUME',
			'UPDATE_VOLUME_NOTE',
			'VIEW_SERIAL_SUBSCRIPTION');


-- Add advanced cataloguing permissions to the Cataloging Admin group

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, TRUE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Cataloging Admin' AND
		aout.name = 'Consortium' AND
		perm.code IN (
			'ADMIN_IMPORT_ITEM_ATTR_DEF',
			'ADMIN_MERGE_PROFILE',
			'CREATE_AUTHORITY_IMPORT_IMPORT_DEF',
			'CREATE_BIB_IMPORT_FIELD_DEF',
			'CREATE_BIB_SOURCE',
			'CREATE_IMPORT_ITEM_ATTR_DEF',
			'CREATE_IMPORT_TRASH_FIELD',
			'CREATE_MERGE_PROFILE',
			'DELETE_AUTHORITY_IMPORT_IMPORT_FIELD_DEF',
			'DELETE_BIB_SOURCE',
			'DELETE_IMPORT_ITEM_ATTR_DEF',
			'DELETE_IMPORT_TRASH_FIELD',
			'DELETE_MERGE_PROFILE',
			'UPDATE_AUTHORITY_IMPORT_IMPORT_FIELD_DEF',
			'UPDATE_BIB_IMPORT_IMPORT_FIELD_DEF',
			'UPDATE_IMPORT_ITEM_ATTR_DEF',
			'UPDATE_IMPORT_TRASH_FIELD',
			'UPDATE_MERGE_PROFILE');

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, TRUE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Cataloging Admin' AND
		aout.name = 'System' AND
		perm.code IN (
			'CREATE_COPY_STAT_CAT',
			'CREATE_COPY_STAT_CAT_ENTRY',
			'CREATE_COPY_STAT_CAT_ENTRY_MAP',
			'RUN_REPORTS',
			'SHARE_REPORT_FOLDER',
			'UPDATE_COPY_LOCATION',
			'UPDATE_COPY_STAT_CAT',
			'UPDATE_COPY_STAT_CAT_ENTRY',
			'VIEW_REPORT_OUTPUT');


-- Add basic circulation permissions to the Circulators group

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Circulators' AND
		aout.name = 'Branch' AND
		perm.code IN (
			'ADMIN_BOOKING_RESERVATION',
			'ADMIN_BOOKING_RESOURCE',
			'ADMIN_BOOKING_RESOURCE_ATTR',
			'ADMIN_BOOKING_RESOURCE_ATTR_MAP',
			'ADMIN_BOOKING_RESOURCE_ATTR_VALUE',
			'ADMIN_BOOKING_RESOURCE_TYPE',
			'ASSIGN_GROUP_PERM',
			'MARK_ITEM_AVAILABLE',
			'MARK_ITEM_BINDERY',
			'MARK_ITEM_CHECKED_OUT',
			'MARK_ITEM_ILL',
			'MARK_ITEM_IN_PROCESS',
			'MARK_ITEM_IN_TRANSIT',
			'MARK_ITEM_LOST',
			'MARK_ITEM_MISSING',
			'MARK_ITEM_ON_HOLDS_SHELF',
			'MARK_ITEM_ON_ORDER',
			'MARK_ITEM_RESHELVING',
			'OFFLINE_UPLOAD',
			'OFFLINE_VIEW',
			'REMOVE_USER_GROUP_LINK',
			'SET_CIRC_CLAIMS_RETURNED',
			'SET_CIRC_CLAIMS_RETURNED.override',
			'SET_CIRC_LOST',
			'SET_CIRC_MISSING',
			'UPDATE_BILL_NOTE',
			'UPDATE_PATRON_CLAIM_NEVER_CHECKED_OUT_COUNT',
			'UPDATE_PATRON_CLAIM_RETURN_COUNT',
			'UPDATE_PAYMENT_NOTE',
			'UPDATE_PICKUP_LIB FROM_TRANSIT',
			'UPDATE_PICKUP_LIB_FROM_HOLDS_SHELF',
			'VIEW_GROUP_PENALTY_THRESHOLD',
			'VIEW_STANDING_PENALTY',
			'VOID_BILLING',
			'VOLUME_HOLDS');

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Circulators' AND
		aout.name = 'System' AND
		perm.code IN (
			'ABORT_REMOTE_TRANSIT',
			'ABORT_TRANSIT',
			'CAPTURE_RESERVATION',
			'CIRC_CLAIMS_RETURNED.override',
			'CIRC_EXCEEDS_COPY_RANGE.override',
			'CIRC_OVERRIDE_DUE_DATE',
			'CIRC_PERMIT_OVERRIDE',
			'COPY_ALERT_MESSAGE.override',
			'COPY_BAD_STATUS.override',
			'COPY_CIRC_NOT_ALLOWED.override',
			'COPY_IS_REFERENCE.override',
			'COPY_NEEDED_FOR_HOLD.override',
			'COPY_NOT_AVAILABLE.override',
			'COPY_STATUS_LOST.override',
			'COPY_STATUS_MISSING.override',
			'CREATE_DUPLICATE_HOLDS',
			'CREATE_USER_GROUP_LINK',
			'DELETE_TRANSIT',
			'HOLD_EXISTS.override',
			'HOLD_ITEM_CHECKED_OUT.override',
			'ISSUANCE_HOLDS',
			'ITEM_AGE_PROTECTED.override',
			'ITEM_ON_HOLDS_SHELF.override',
			'MAX_RENEWALS_REACHED.override',
			'OVERRIDE_HOLD_HAS_LOCAL_COPY',
			'PATRON_EXCEEDS_CHECKOUT_COUNT.override',
			'PATRON_EXCEEDS_FINES.override',
			'PATRON_EXCEEDS_OVERDUE_COUNT.override',
			'RETRIEVE_RESERVATION_PULL_LIST',
			'UPDATE_HOLD');


-- Add advanced circulation permissions to the Circulation Admin group

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, TRUE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Circulation Admin' AND
		aout.name = 'Branch' AND
		perm.code IN (
			'DELETE_USER');

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, TRUE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Circulation Admin' AND
		aout.name = 'Consortium' AND
		perm.code IN (
			'ADMIN_MAX_FINE_RULE',
			'CREATE_CIRC_DURATION',
			'DELETE_CIRC_DURATION',
			'UPDATE_CIRC_DURATION',
			'UPDATE_NET_ACCESS_LEVEL',
			'VIEW_CIRC_MATRIX_MATCHPOINT',
			'VIEW_HOLD_MATRIX_MATCHPOINT');

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, TRUE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Circulation Admin' AND
		aout.name = 'System' AND
		perm.code IN (
			'ADMIN_BOOKING_RESERVATION',
			'ADMIN_BOOKING_RESERVATION_ATTR_MAP',
			'ADMIN_BOOKING_RESERVATION_ATTR_VALUE_MAP',
			'ADMIN_BOOKING_RESOURCE',
			'ADMIN_BOOKING_RESOURCE_ATTR',
			'ADMIN_BOOKING_RESOURCE_ATTR_MAP',
			'ADMIN_BOOKING_RESOURCE_ATTR_VALUE',
			'ADMIN_BOOKING_RESOURCE_TYPE',
			'ADMIN_COPY_LOCATION_ORDER',
			'ADMIN_HOLD_CANCEL_CAUSE',
			'ASSIGN_GROUP_PERM',
			'BAR_PATRON',
			'COPY_HOLDS',
			'COPY_TRANSIT_RECEIVE',
			'CREATE_BILL',
			'CREATE_BILLING_TYPE',
			'CREATE_NON_CAT_TYPE',
			'CREATE_PATRON_STAT_CAT',
			'CREATE_PATRON_STAT_CAT_ENTRY',
			'CREATE_PATRON_STAT_CAT_ENTRY_MAP',
			'CREATE_USER_GROUP_LINK',
			'DELETE_BILLING_TYPE',
			'DELETE_NON_CAT_TYPE',
			'DELETE_PATRON_STAT_CAT',
			'DELETE_PATRON_STAT_CAT_ENTRY',
			'DELETE_PATRON_STAT_CAT_ENTRY_MAP',
			'DELETE_TRANSIT',
			'group_application.user.staff',
			'MANAGE_BAD_DEBT',
			'MARK_ITEM_AVAILABLE',
			'MARK_ITEM_BINDERY',
			'MARK_ITEM_CHECKED_OUT',
			'MARK_ITEM_ILL',
			'MARK_ITEM_IN_PROCESS',
			'MARK_ITEM_IN_TRANSIT',
			'MARK_ITEM_LOST',
			'MARK_ITEM_MISSING',
			'MARK_ITEM_ON_HOLDS_SHELF',
			'MARK_ITEM_ON_ORDER',
			'MARK_ITEM_RESHELVING',
			'MERGE_USERS',
			'money.collections_tracker.create',
			'money.collections_tracker.delete',
			'OFFLINE_EXECUTE',
			'OFFLINE_UPLOAD',
			'OFFLINE_VIEW',
			'REMOVE_USER_GROUP_LINK',
			'SET_CIRC_CLAIMS_RETURNED',
			'SET_CIRC_CLAIMS_RETURNED.override',
			'SET_CIRC_LOST',
			'SET_CIRC_MISSING',
			'UNBAR_PATRON',
			'UPDATE_BILL_NOTE',
			'UPDATE_NON_CAT_TYPE',
			'UPDATE_PATRON_CLAIM_NEVER_CHECKED_OUT_COUNT',
			'UPDATE_PATRON_CLAIM_RETURN_COUNT',
			'UPDATE_PICKUP_LIB_FROM_HOLDS_SHELF',
			'UPDATE_PICKUP_LIB_FROM_TRANSIT',
			'UPDATE_USER',
			'VIEW_REPORT_OUTPUT',
			'VIEW_STANDING_PENALTY',
			'VOID_BILLING',
			'VOLUME_HOLDS');


-- Add basic sys admin permissions to the Local Administrator group

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, TRUE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Local Administrator' AND
		aout.name = 'Branch' AND
		perm.code IN (
			'EVERYTHING');


-- Add administration permissions to the System Administrator group

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, TRUE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'System Administrator' AND
		aout.name = 'System' AND
		perm.code IN (
			'EVERYTHING');

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'System Administrator' AND
		aout.name = 'Consortium' AND
		perm.code ~ '^VIEW_TRIGGER';


-- Add administration permissions to the Global Administrator group

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, TRUE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Global Administrator' AND
		aout.name = 'Consortium' AND
		perm.code IN (
			'EVERYTHING');


-- Add basic acquisitions permissions to the Acquisitions group

SELECT SETVAL('permission.grp_perm_map_id_seq'::TEXT, (SELECT MAX(id) FROM permission.grp_perm_map));

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Acquisitions' AND
		aout.name = 'Consortium' AND
		perm.code IN (
			'ALLOW_ALT_TCN',
			'CREATE_BIB_IMPORT_QUEUE',
			'CREATE_IMPORT_ITEM',
			'CREATE_INVOICE',
			'CREATE_MARC',
			'CREATE_PICKLIST',
			'CREATE_PURCHASE_ORDER',
			'DELETE_BIB_IMPORT_QUEUE',
			'DELETE_IMPORT_ITEM',
			'DELETE_RECORD',
			'DELETE_VOLUME',
			'DELETE_VOLUME_NOTE',
			'GENERAL_ACQ',
			'IMPORT_ACQ_LINEITEM_BIB_RECORD',
			'IMPORT_MARC',
			'MANAGE_CLAIM',
			'MANAGE_FUND',
			'MANAGE_FUNDING_SOURCE',
			'MANAGE_PROVIDER',
			'MARK_ITEM_AVAILABLE',
			'MARK_ITEM_BINDERY',
			'MARK_ITEM_CHECKED_OUT',
			'MARK_ITEM_ILL',
			'MARK_ITEM_IN_PROCESS',
			'MARK_ITEM_IN_TRANSIT',
			'MARK_ITEM_LOST',
			'MARK_ITEM_MISSING',
			'MARK_ITEM_ON_HOLDS_SHELF',
			'MARK_ITEM_ON_ORDER',
			'MARK_ITEM_RESHELVING',
			'RECEIVE_PURCHASE_ORDER',
			'UPDATE_BATCH_COPY',
			'UPDATE_BIB_IMPORT_QUEUE',
			'UPDATE_COPY',
			'UPDATE_FUND',
			'UPDATE_FUND_ALLOCATION',
			'UPDATE_FUNDING_SOURCE',
			'UPDATE_IMPORT_ITEM',
			'UPDATE_MARC',
			'UPDATE_RECORD',
			'UPDATE_VOLUME',
			'user_request.delete',
			'user_request.update',
			'user_request.view',
			'VIEW_ACQ_FUND_ALLOCATION_PERCENT',
			'VIEW_ACQ_FUNDING_SOURCE',
			'VIEW_FUND',
			'VIEW_FUND_ALLOCATION',
			'VIEW_FUNDING_SOURCE',
			'VIEW_HOLDS',
			'VIEW_INVOICE',
			'VIEW_ORG_SETTINGS',
			'VIEW_PICKLIST',
			'VIEW_PROVIDER',
			'VIEW_PURCHASE_ORDER',
			'VIEW_REPORT_OUTPUT');


-- Add acquisitions administration permissions to the Acquisitions Admin group

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, TRUE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Acquisitions Administrator' AND
		aout.name = 'Consortium' AND
		perm.code IN (
			'ACQ_XFER_MANUAL_DFUND_AMOUNT',
			'ADMIN_ACQ_CANCEL_CAUSE',
			'ADMIN_ACQ_CLAIM',
			'ADMIN_ACQ_CLAIM_EVENT_TYPE',
			'ADMIN_ACQ_CLAIM_TYPE',
			'ADMIN_ACQ_DISTRIB_FORMULA',
			'ADMIN_ACQ_FISCAL_YEAR',
			'ADMIN_ACQ_FUND',
			'ADMIN_ACQ_FUND_ALLOCATION_PERCENT',
			'ADMIN_ACQ_FUND_TAG',
			'ADMIN_ACQ_LINE_ITEM_ALERT_TEXT',
			'ADMIN_CLAIM_POLICY',
			'ADMIN_CURRENCY_TYPE',
			'ADMIN_FUND',
			'ADMIN_FUNDING_SOURCE',
			'ADMIN_INVOICE',
			'ADMIN_INVOICE_METHOD',
			'ADMIN_INVOICE_PAYMENT_METHOD',
			'ADMIN_LINEITEM_MARC_ATTR_DEF',
			'ADMIN_PROVIDER',
			'ADMIN_USER_REQUEST_TYPE',
			'CREATE_ACQ_FUNDING_SOURCE',
			'CREATE_FUND',
			'CREATE_FUND_ALLOCATION',
			'CREATE_FUNDING_SOURCE',
			'CREATE_INVOICE_ITEM_TYPE',
			'CREATE_INVOICE_METHOD',
			'CREATE_PROVIDER',
			'DELETE_ACQ_FUNDING_SOURCE',
			'DELETE_FUND',
			'DELETE_FUND_ALLOCATION',
			'DELETE_FUNDING_SOURCE',
			'DELETE_INVOICE_ITEM_TYPE',
			'DELETE_INVOICE_METHOD',
			'DELETE_PROVIDER',
			'RUN_REPORTS',
			'SHARE_REPORT_FOLDER',
			'UPDATE_ACQ_FUNDING_SOURCE',
			'UPDATE_INVOICE_ITEM_TYPE',
			'UPDATE_INVOICE_METHOD');


-- Add serials permissions to the Serials group

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Serials' AND
		aout.name = 'System' AND
		perm.code IN (
			'ADMIN_ASSET_COPY_TEMPLATE',
			'ADMIN_SERIAL_CAPTION_PATTERN',
			'ADMIN_SERIAL_DISTRIBUTION',
			'ADMIN_SERIAL_STREAM',
			'ADMIN_SERIAL_SUBSCRIPTION',
			'ISSUANCE_HOLDS',
			'RECEIVE_SERIAL');


-- Add basic staff permissions to the Volunteers group

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Volunteers' AND
		aout.name = 'Branch' AND
		perm.code IN (
			'COPY_CHECKOUT',
			'CREATE_BILL',
			'CREATE_IN_HOUSE_USE',
			'CREATE_PAYMENT',
			'VIEW_BILLING_TYPE',
			'VIEW_CIRCS',
			'VIEW_COPY_CHECKOUT',
			'VIEW_HOLD',
			'VIEW_TITLE_HOLDS',
			'VIEW_TRANSACTION',
			'VIEW_USER',
			'VIEW_USER_FINES_SUMMARY',
			'VIEW_USER_TRANSACTIONS');

INSERT INTO permission.grp_perm_map (grp, perm, depth, grantable)
	SELECT
		pgt.id, perm.id, aout.depth, FALSE
	FROM
		permission.grp_tree pgt,
		permission.perm_list perm,
		actor.org_unit_type aout
	WHERE
		pgt.name = 'Volunteers' AND
		aout.name = 'Consortium' AND
		perm.code IN (
			'CREATE_COPY_TRANSIT',
			'CREATE_TRANSACTION',
			'CREATE_TRANSIT',
			'STAFF_LOGIN',
			'TRANSIT_COPY',
			'VIEW_ORG_SETTINGS');

