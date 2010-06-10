#!/usr/bin/perl -w

# Converts a Unicorn users.data file to a tab-separated file.
# 2009-08-10 Ben Ostrowsky <ben@esilibrary.com>

#use Data::Dumper;
#$Data::Dumper::Sortkeys = true;

my @records;
my $serial = -1;
my $line = 0;
my $section = '';
my $field = '';
my %unique_fields;
my @output_fields = qw(
l_user_id l_user_altid l_user_pin l_user_profile l_user_status l_user_library l_user_priv_granted 
l_user_priv_expires l_user_mailingaddr l_birthdate l_prefix_name l_last_name l_first_name l_middle_name 
l_suffix_name l_note l_note1 l_patron l_comment l_staff l_webcatpref l_user_category1 l_user_category2 
l_user_category3 l_user_category4 l_dept l_guardian l_license l_ssn l_misc l_aup l_photo l_notify_via 
l_user_claims_ret l_user_environment l_user_department l_ums_id l_user_last_activity l_placcard l_user_email 
l_addr1_std_line1 l_addr1_std_line2 l_addr1_std_city l_addr1_std_state l_addr1_std_zip l_addr1_country l_addr1_county
l_addr1_township l_addr1_room l_addr1_company l_addr1_office l_addr1_phone l_addr1_dayphone l_addr1_homephone 
l_addr1_workphone l_addr1_cellphone l_addr1_fax l_addr1_email l_addr1_location l_addr1_usefor l_addr1_care_of 
l_addr1_known_bad l_addr1_ums_addrid l_addr2_std_line1 l_addr2_std_line2 l_addr2_std_city l_addr2_std_state 
l_addr2_std_zip l_addr2_country l_addr2_county l_addr2_township l_addr2_room l_addr2_company l_addr2_office l_addr2_phone 
l_addr2_dayphone l_addr2_homephone l_addr2_workphone l_addr2_cellphone l_addr2_fax l_addr2_email 
l_addr2_location l_addr2_usefor l_addr2_care_of l_addr2_known_bad l_addr2_ums_addrid l_addr3_std_line1 
l_addr3_std_line2 l_addr3_std_city l_addr3_std_state l_addr3_std_zip l_addr3_country l_addr3_county l_addr3_township 
l_addr3_room l_addr3_company l_addr3_office l_addr3_phone l_addr3_dayphone l_addr3_homephone l_addr3_workphone 
l_addr3_cellphone l_addr3_fax l_addr3_email l_addr3_location l_addr3_usefor l_addr3_care_of l_addr3_known_bad 
l_addr3_ums_addrid l_identific l_noempl l_profession l_program l_represent l_userid_active l_inactive_barcode1 
l_inactive_barcode2);


# Load each record
while (<>) {
    s/\r\n/\n/g;
# print STDERR "Loaded this line: " . $_;

	# Is this the start of a new record?
	if ( /^... DOCUMENT BOUNDARY ...$/ ) {
		$line = 0;
		$serial++;
		$section = ''; # just in case this didn't get reset in the previous record
		# print STDERR "Processing record $serial.\n";
		next;
	}

	# Is this a FORM= line (which can be ignored)?
	if ( /^FORM=/ ) {
		next;
	}

	# If this isn't the start of the new record, it's a new line in the present record.
	$line++;

	# Is this line the beginning of a block of data (typically an address or a note)?
	if ( /^\.(.*?)_BEGIN.$/ ) {
		# print STDERR "I think this might be the beginning of a beautiful " . $1 . ".\n";
		$section = "$1.";
		next;
	}

	# Is this line the end of a block of data (typically an address or a note)?
	if ( /^\.(.*?)_END.$/ ) {
		if ("$1." ne $section) {
			print STDERR "Error in record $serial, line $line (input line $.): got an end-of-$1 but I thought I was in a $section block!\n";
		}
		# print STDERR "It's been fun, guys, but... this is the end of the " . $1 . ".\n";
		$section = '';
		next;
	}

	# Looks like we've got some actual data!  Let's store it.
	# FIXME: For large batches of data, we may run out of memory and should store this on disk.
	if ( /^\.(.*?).\s+(\|a)?(.*)$/ ) {

		# Build the name of this field (taking note of whether we're in a named section of data)
		$field = '';
		if ($section ne '') { 
			$field .= $section;
		}
		$field .= $1;

		# Store the field as a key of an array.  If it already exists, oh well, now it still exists.
		$unique_fields{$field} = 1;

		# Now we can actually store this line of data!
		$records[$serial]{$field} = $3;		

		# print STDERR "Data extracted: \$records[$serial]{'$field'} = '$3'\n";

		next;
	}	

	# This is the continuation of the previous line.
	else {
		chomp($_);
		$records[$serial]{$field} .= ' ' . $_;
		# print STDERR "Appended data to previous field. \$records[$serial]{'$field'} is now '" . $records[$serial]{$field} . "'.\n";
	}

}

print STDERR "Loaded " . scalar(@records) . " records.\n";


# Process the records:
for (my $u = 0; $u < @records; $u++) {

#print STDERR "Before processing:\n\n";
#print STDERR Dumper($records[$u]);

	# Some fields can be mapped straightforwardly:
	foreach $f (qw( user_id user_alt_id user_pin user_profile user_status user_library user_priv_granted user_priv_expires user_mailingaddr user_claims_ret user_environment user_department user_last_activity user_category1 user_category2 user_category3 user_category4 )) {
		$records[$u]{uc($f)} = '' unless defined $records[$u]{uc($f)};
		$records[$u]{'l_' . $f} = $records[$u]{uc($f)};
	}

	# Addresses are a bit different:
	foreach $a (qw( addr1 addr2 addr3 )) {
		foreach $f (qw( std_line1 std_line2 std_city std_state std_zip country county township room company office phone dayphone homephone workphone cellphone fax email location usefor care_of known_bad ums_addrid )) {
			$records[$u]{uc('USER_' . $a . '.' . $f)} = '' unless defined $records[$u]{uc('USER_' . $a . '.' . $f)};
			$records[$u]{'l_' . $a . '_' . $f} = $records[$u]{uc('USER_' . $a . '.' . $f)};
		}
		$records[$u]{'l_' . $a . '_std_line1'} = $records[$u]{'USER_' . uc($a) . '.STREET'};
		if ((defined $records[$u]{'USER_' . uc($a) . '.CITY/STATE'}) && ($records[$u]{'USER_' . uc($a) . '.CITY/STATE'} =~ m/^(.*), (.*)$/)) {
			$records[$u]{'l_' . $a . '_std_city'} = $1;
			$records[$u]{'l_' . $a . '_std_state'} = $2;
		}
		$records[$u]{'l_' . $a . '_std_zip'} = $records[$u]{'USER_' . uc($a) . '.ZIP'};

	}

	# Handle fields that don't exactly match (e.g. parse USER_NAME into l_last_name etc.)

	$records[$u]{'l_birthdate'} = $records[$u]{'USER_BIRTH_DATE'};
	$records[$u]{'l_note'} = $records[$u]{'USER_XINFO.NOTE'};
	$records[$u]{'l_note1'} = '';
	$records[$u]{'l_patron'} = '';
	$records[$u]{'l_comment'} = $records[$u]{'USER_XINFO.COMMENT'};
	$records[$u]{'l_staff'} = $records[$u]{'USER_XINFO.STAFF'};
	$records[$u]{'l_webcatpref'} = $records[$u]{'USER_XINFO.WEBCATPREF'};
	$records[$u]{'l_dept'} = $records[$u]{'USER_DEPARTMENT'};
	$records[$u]{'l_guardian'} = '';
	$records[$u]{'l_license'} = $records[$u]{'USER_XINFO.LICENSE'};
	$records[$u]{'l_ssn'} = $records[$u]{'USER_XINFO.SSN'};
	$records[$u]{'l_misc'} = '';
	$records[$u]{'l_aup'} = '';
	$records[$u]{'l_photo'} = '';
	$records[$u]{'l_notify_via'} = $records[$u]{'USER_XINFO.NOTIFY_VIA'};
	$records[$u]{'l_ums_id'} = '';
	$records[$u]{'l_placcard'} = '';
	$records[$u]{'l_user_email'} = '';
	$records[$u]{'l_identific'} = '';
	$records[$u]{'l_noempl'} = '';
	$records[$u]{'l_profession'} = '';
	$records[$u]{'l_program'} = '';
	$records[$u]{'l_represent'} = '';
	$records[$u]{'l_userid_active'} = '';
	$records[$u]{'l_inactive_barcode1'} = $records[$u]{'USER_XINFO.PREV_ID'};
	$records[$u]{'l_inactive_barcode2'} = $records[$u]{'USER_XINFO.PREV_ID2'};
	$records[$u]{'l_user_category1'} = $records[$u]{'USER_ROUTING_FLAG'};
	$records[$u]{'l_user_category2'} = $records[$u]{'USER_WEB_AUTH'};

	# We can parse the name like so:

	# Copy the name to a temp value
	$temp_name = $records[$u]{'USER_NAME'};

	# If there's no comma, don't try to parse the name
	unless ($temp_name =~ m/,/) {
		$records[$u]{'l_last_name'} = $temp_name;
		next;
	}

	# Strip off a prefix, if there is one
	foreach $prefix (qw( Ms\. Mrs\. Mr\. Dr\. )) {
		if ($temp_name =~ /$prefix /i) {
			$records[$u]{'l_prefix_name'} = $prefix;
			$temp_name =~ s/$prefix //i;
		}
	}

	# Strip off a suffix, if there is one
	foreach $suffix (qw( Jr\. Jr Sr\. Sr III II IV )) {
		if ($temp_name =~ / $suffix/i) {
			$records[$u]{'l_suffix_name'} = $suffix;
			$temp_name =~ s/ $suffix//i;
		}
	}

	# Strip off the family name (before the comma)
	# Of what remains, whatever is before the first space is the first name and the rest is the middle name
	$temp_name =~ m/^([^,]*)\s*,.*$/;
	$records[$u]{'l_last_name'} = $1;
	$temp_name =~ m/^[^,]*\s*,\s*([^,\s]*)\s*(.*)$/;
	$records[$u]{'l_first_name'} = $1;
	$records[$u]{'l_middle_name'} = $2;

#print STDERR "\n\nAfter processing:\n\n";
#print STDERR Dumper($records[$u]);


}


# Print the results
for (my $u = 0; $u < @records; $u++) {
	foreach $f (@output_fields) {
		if (defined $records[$u]{$f}) {
			print $records[$u]{$f};
		}
	print "\t";
	}
	print "\n";
}

print STDERR "Wrote " . scalar(@records) . " records.\n";
# uh-bdee-uh-bdee-uh-bdee-uh- THAT'S ALL, FOLKS
