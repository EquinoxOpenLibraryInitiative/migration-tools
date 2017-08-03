#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;
use Getopt::Long;
use XML::LibXML;
use Switch;
use open ':encoding(utf8)';

select(STDERR);
$| = 1;
select(STDOUT); 
$| = 1;

my $mig_schema;
my $ldif_xml;

my $ret = GetOptions(
    'ldif_xml:s'     => \$ldif_xml
);
if (!defined $ldif_xml) {$ldif_xml = 'export.xml'};

print "Parsing LDIF ... ";
my $parser = XML::LibXML->new();
my $dom = $parser->parse_file($ldif_xml);
print "done.\n\n";

user_defined_values($dom,'user_defined_values.tsv');
asset_copy_locations($dom,'asset_copy_location_legacy.tsv');
actor_usr_groups($dom,'actor_usr_legacy_groups.tsv');
actor_usr($dom,'actor_usr_legacy.tsv','actor_usr_address_legacy.tsv','actor_usr_phones.tsv','actor_usr_note_legacy.tsv','money_billing_legacy.tsv');
biblio_record_entry($dom,'biblio_record_entry_legacy.tsv');
action_uses($dom,'action_in_house_use_legacy.tsv','in-library'); 
action_uses($dom,'action_circulation_legacy.tsv','circulation');
action_uses($dom,'action_transit_copy_legacy.tsv','transit');
action_uses($dom,'action_hold_request_shelf.tsv','reserve');
action_hold_request($dom,'action_hold_request_legacy.tsv');
asset_copy($dom,'asset_copy_legacy.tsv');
asset_copy_note($dom,'asset_copy_note_legacy.tsv');

print "Staging tables conversion completed.\n\n";

#################################################################
sub user_defined_values {
    my $dom = shift;
    my $outfile = shift;

    print "Parsing for user defined values... ";
    my @user_defined; 
    foreach my $def ($dom->findnodes('//userdef')) {
        push @user_defined, {
            id          => $def->findvalue('./@id'),
            record_type => $def->findvalue('./@record'),
            name        => $def->findvalue('./@name'),
            type        => $def->findvalue('./@type')
        };  
    }
   
    my $num_rows = @user_defined;
        
    open my $out_fh, ">:utf8", $outfile or die "can not open $outfile file for writing.\n";

    print "writing $outfile ... ";

    print $out_fh "id\trecord_type\tname\ttype\n";

    foreach my $built_hash( @user_defined ) { 
        print $out_fh "$built_hash->{id}\t";
        print $out_fh "$built_hash->{record_type}\t";
        print $out_fh "$built_hash->{name}\t";
        print $out_fh "$built_hash->{type}\n";
    }   

    close $out_fh;
    print "$num_rows rows written plus headers.\n";
}

sub asset_copy_note {
    my $dom = shift;
    my $outfile = shift;

    print "Parsing for holdings values... ";
    my @holdings;
    foreach my $copy ($dom->findnodes('//holding')) {
        if (length($copy->findvalue('./@deleted')) > 1) { next; }
        foreach my $note ($copy->findnodes('.//holdingNote')) {
            push @holdings, {
                copy_id => $copy->findvalue('./@id'),
                active  => $note->findvalue('./@active'),
                note    => $note->findvalue('./@note')
            };
        }
    }

    my $num_rows = @holdings;

    open my $out_fh, ">:utf8", $outfile or die "can not open $outfile file for writing.\n";
    print "writing $outfile ... ";
    print $out_fh "copy_id\tactive\tnote\n";
    foreach my $built_hash( @holdings ) {
        print $out_fh "$built_hash->{copy_id}\t";
        print $out_fh "$built_hash->{active}\t";
        print $out_fh "$built_hash->{note}\n";
    }

    close $out_fh;
    print "$num_rows rows written plus headers.\n";
}

sub asset_copy {
    my $dom = shift;
    my $outfile = shift;

    print "Parsing for holdings values... ";
    my @holdings;
    foreach my $copy ($dom->findnodes('//holding')) {
        if (length($copy->findvalue('./@deleted')) > 1) { next; }
        my $memberships;
        foreach my $mship ($copy->findnodes('./membership')) {
            if (defined $memberships) { $memberships = $memberships . '|' . $mship->to_literal; }
                else { $memberships = $mship->to_literal; }
        }
        push @holdings, {
            id           => $copy->findvalue('./@id'),
            memberships  => $memberships,
            location     => $copy->findvalue('./@location'),
            deleted_type => $copy->findvalue('./@deletedType'),
            status       => $copy->findvalue('./@status'),
            deleted      => $copy->findvalue('./@deleted'),
            barcode      => $copy->findvalue('./@barcode'),
            biblio       => $copy->findvalue('./@biblio'),
            usage_count  => $copy->findvalue('./@usageCount'),
            price_cents  => $copy->findvalue('./@priceCents'),
            call         => $copy->findvalue('./@call'),
            edited       => $copy->findvalue('./@edited')
        }; 
    }
  
    my $num_rows = @holdings;

    open my $out_fh, ">:utf8", $outfile or die "can not open $outfile file for writing.\n";
    print "writing $outfile ... ";
    print $out_fh "id\tmemberships\tlocation\tdeleted_type\tstatus\tdeleted\tbarcode\tbiblio\tusage_count\tprice_cents\tcall\tedited\n";
    foreach my $built_hash( @holdings ) {
        print $out_fh "$built_hash->{id}\t";
        print $out_fh "$built_hash->{memberships}\t";
        print $out_fh "$built_hash->{location}\t";
        print $out_fh "$built_hash->{deleted_type}\t";
        print $out_fh "$built_hash->{status}\t";
        print $out_fh "$built_hash->{deleted}\t";
        print $out_fh "$built_hash->{barcode}\t";
        print $out_fh "$built_hash->{biblio}\t";
        print $out_fh "$built_hash->{usage_count}\t";
        print $out_fh "$built_hash->{price_cents}\t";
        print $out_fh "$built_hash->{call}\t";
        print $out_fh "$built_hash->{edited}\n";
    }  

    close $out_fh;
    print "$num_rows rows written plus headers.\n";
}

sub action_hold_request {
    my $dom = shift;
    my $out_file = shift;

    print "Parsing users data for hold requests ... ";
    my @holds;
    foreach my $patron ($dom->findnodes('///patron')) {
        foreach my $request ($patron->findnodes('.//reserve')) {
            if (length($request->findvalue('./@resolved')) > 1) { next; }
            push @holds, {   #patron id, reserve id should be redundant but leaving in for error checking
                id => $request->findvalue('./@id'),
                patron_barcode => $patron->findvalue('./@barcode'),
                patron_id => $patron->findvalue('./@id'),
                biblio => $request->findvalue('./@biblio'),
                status => $request->findvalue('./@status'),
                resolved => $request->findvalue('./@resolved'),
                placed => $request->findvalue('./@placed'),
            };
        }
    }

    my $num_rows = @holds;
    open my $out_fh, ">:utf8", $out_file or die "can not open $out_file file for writing.\n";
    print "writing $out_file ... \n";
    print $out_fh "id\tpatron_barcode\tpatron_id\tbiblio\tstatus\tresolved\tplaced\n";
    foreach my $built_hash( @holds ) {
        print $out_fh "$built_hash->{id}\t";
        print $out_fh "$built_hash->{patron_barcode}\t";
        print $out_fh "$built_hash->{patron_id}\t";
        print $out_fh "$built_hash->{biblio}\t";
        print $out_fh "$built_hash->{status}\t";
        print $out_fh "$built_hash->{resolved}\t";
        print $out_fh "$built_hash->{placed}\n";
    }
    close $out_fh;
    print "$num_rows rows written plus headers to $out_file.\n";
}

sub asset_copy_locations {
    my $dom = shift;
    my $outfile = shift;

    print "Parsing for copy locations... ";
    my @copy_locations;  
    foreach my $membershiplist ($dom->findnodes('///holdingMembershipList')) {
        my $membershiplist_id = $membershiplist->findvalue('./@id'); 
        my $membershiplist_name = $membershiplist->findvalue('./@name');
        foreach my $membership ($membershiplist->findnodes('./holdingMembership')) {
            push @copy_locations, {
                membership_list_id   => $membershiplist_id,
                membership_list_name => $membershiplist_name,
                membership_number    => $membership->findvalue('./@number'),
                membership_name      => $membership->findvalue('./@name'),
                membership_id        => $membership->findvalue('./@id')
            };
        }
    } 

    my $num_rows = @copy_locations;
    
    open my $out_fh, ">:utf8", $outfile or die "can not open $outfile file for writing.\n";

    print "writing $outfile ... ";

    #print headers
    print $out_fh "membership_list_id\tmembership_list_name\tmembership_id\tmembership_number\tmembership_name\n";

    foreach my $built_hash( @copy_locations ) {
        print $out_fh "$built_hash->{membership_list_id}\t";
        print $out_fh "$built_hash->{membership_list_name}\t";
        print $out_fh "$built_hash->{membership_number}\t";
        print $out_fh "$built_hash->{membership_name}\t";
        print $out_fh "$built_hash->{membership_id}\n";
    }

    close $out_fh;
    print "$num_rows rows written plus headers.\n";
}


sub action_uses {
    my $dom = shift;
    my $outfile = shift;
    my $table_type = shift;

    switch ($table_type) {
        case 'transit'      {print "Parsing for copy transits ... ";}
        case 'in-library'   {print "Parsing for in house uses ... ";}
        case 'circulation'  {print "Parsing for circulations ... ";}
    }

    my @checkouts;  #not exporting the memberships since we have the patron id to link
    foreach my $circ ($dom->findnodes('//checkout')) {
        my $renewals = 0;
        if ($table_type eq 'circulation') {
            if ($circ->findvalue('./@type') ne 'normal') { next; }
            if (length($circ->findvalue('./@due')) < 1) { next; } #bunch of records with no due dates, cause not clear
            if (length($circ->findvalue('./@returned')) > 0) { next; }
        }
        if ($table_type eq 'transit') {
            if ($circ->findvalue('./@type') ne 'transt') { next; }
            if (length($circ->findvalue('./@returned')) > 0) { next; }
        }
        if ($table_type eq 'in-library') {
            if ($circ->findvalue('./@type') ne 'in-library') { next; }
        }
        if ($table_type eq 'reserve') {
            if ($circ->findvalue('./@type') ne 'reserve') { next; } 
            if ($circ->findvalue('./@status') ne 'out') { next; }
        }

        foreach ($circ->findnodes('./renewal')) { 
            $renewals++;  #normally don't move move over previous circs in renewal sequence so just counting them
        }
        push @checkouts, {
            id                  => $circ->findvalue('./@id'),
            patron              => $circ->findvalue('./@patron'),
            out                 => $circ->findvalue('./@out'),
            due                 => $circ->findvalue('./@due'),
            status              => $circ->findvalue('./@status'),
            holding             => $circ->findvalue('./@holding'),
            returned            => $circ->findvalue('./@returned'),
            type                => $circ->findvalue('./@type'),
            out_location        => $circ->findvalue('./@outLocation'),
            returned_location   => $circ->findvalue('./@returnedLocation'),
            reserve_id          => $circ->findvalue('./@reserveId'),
            self_check          => $circ->findvalue('./@selfCheck'),
            renewals            => $renewals
        };  
    }   

    my $num_rows = @checkouts;
            
    open my $out_fh, ">:utf8", $outfile or die "can not open $outfile file for writing.\n";

    print "writing $outfile ... ";
    
    print $out_fh "id\tpatron\tout\tdue\tstatus\tholding\treturned\ttype\tout_location\treturned_location\treserve_id\tself_check\trenewals\n";

    foreach my $built_hash( @checkouts ) { 
        print $out_fh "$built_hash->{id}\t";
        print $out_fh "$built_hash->{patron}\t";
        print $out_fh "$built_hash->{out}\t";
        print $out_fh "$built_hash->{due}\t";
        print $out_fh "$built_hash->{status}\t";
        print $out_fh "$built_hash->{holding}\t";
        print $out_fh "$built_hash->{returned}\t";
        print $out_fh "$built_hash->{type}\t";
        print $out_fh "$built_hash->{out_location}\t";
        print $out_fh "$built_hash->{returned_location}\t";
        print $out_fh "$built_hash->{reserve_id}\t";
        print $out_fh "$built_hash->{self_check}\t";
        print $out_fh "$built_hash->{renewals}\n";
    }   
    close $out_fh;
    print "$num_rows rows written plus headers.\n";

}

sub actor_usr_groups {
    my $dom = shift;
    my $outfile = shift;

    print "Parsing for user groups... ";
    my @user_groups;  
    foreach my $membershiplist ($dom->findnodes('///patronMembershipList')) {

        my $membershiplist_id = $membershiplist->findvalue('./@id'); 
        my $membershiplist_name = $membershiplist->findvalue('./@name');
        foreach my $membership ($membershiplist->findnodes('./patronMembership')) {
            push @user_groups, {
                membership_list_id   => $membershiplist_id,
                membership_list_name => $membershiplist_name,
                membership_length_months    => $membership->findvalue('./@membershipLengthMonths'),
                membership_name      => $membership->findvalue('./@name'),
                membership_id        => $membership->findvalue('./@id'),
                membership_number    => $membership->findvalue('./@number')
            };  
        }   
    }   

    my $num_rows = @user_groups;
        
    open my $out_fh, ">:utf8", $outfile or die "can not open $outfile file for writing.\n";

    print "writing $outfile ... ";

    print $out_fh "membership_list_id\tmembership_list_name\tmembership_id\tmembership_number\tmembership_name\tmembership_length_months\n";

    foreach my $built_hash( @user_groups ) { 
        print $out_fh "$built_hash->{membership_list_id}\t";
        print $out_fh "$built_hash->{membership_list_name}\t";
        print $out_fh "$built_hash->{membership_number}\t";
        print $out_fh "$built_hash->{membership_name}\t";
        print $out_fh "$built_hash->{membership_id}\t";
        print $out_fh "$built_hash->{membership_length_months}\n";
    }

    close $out_fh;
    print "$num_rows rows written plus headers.\n";
}

sub actor_usr {
    my $dom = shift;
    my $out_user_file = shift;
    my $out_address_file = shift;
    my $out_phone_file = shift;
    my $out_note_file = shift;
    my $out_fine_file = shift;

    my @users; 
    my @user_addresses;
    my @user_phones;
    my @user_notes;
    my @patron_fines;

    print "Parsing users for addresses, patrons, fines, notes, etc... \n";
    foreach my $patron ($dom->findnodes('///patron')) {
        my $first_names;
        my $middle_names;
        my $preferred_names;
        my $birthdates;
        foreach my $f_name ($patron->findnodes('.//firstName')) {
            if (defined $first_names) { $first_names = $first_names . '|' . $f_name->findvalue('./@name'); }
                else { $first_names = $f_name->findvalue('./@name'); }
            if (defined $birthdates) { $birthdates = $birthdates . '|' . $f_name->findvalue('./@birthdate'); }
                else { $birthdates = $f_name->findvalue('./@birthdate'); }
            if (defined $middle_names) { $middle_names = $middle_names . '|' . $f_name->findvalue('./@middleName'); }
                else { $middle_names = $f_name->findvalue('./@middleName'); }
            if (defined $preferred_names) { $preferred_names = $preferred_names . '|' . $f_name->findvalue('./@preferredName'); }
                else { $preferred_names = $f_name->findvalue('./@preferredName'); }
        }       
        my $memberships;
        foreach my $mship ($patron->findnodes('.//membership')) {
            if (defined $memberships) { $memberships = $memberships . '|' . $mship->to_literal; }
                else { $memberships = $mship->to_literal; }
        }
        my $emails;  
        foreach my $email ($patron->findnodes('.//email')) {
            if (defined $emails) { $emails = $emails . ',' . $email->findvalue('./@address'); }
                else { $emails = $email->findvalue('./@address'); }
        }  
        #lazy code, do programatically later
        my $u3710;
        my $u3714;
        foreach my $ud ($patron->findnodes('./userdefVal')) {
            if ($ud->findvalue('./@uID') eq 'u3710') { $u3710 = $ud->findvalue('./@value')};
            if ($ud->findvalue('./@uID') eq 'u3714') { $u3714 = $ud->findvalue('./@value')};
        } 
        push @users, {
            patron_barcode => $patron->findvalue('./@barcode'),
            created => $patron->findvalue('./@created'),
            usage_count => $patron->findvalue('./@usageCount'),
            family_id => $patron->findvalue('./@familyID'),
            expiration => $patron->findvalue('./@expiration'),
            patron_id => $patron->findvalue('./@id'),
            last_name => $patron->findvalue('./@lastName'),
            edited => $patron->findvalue('./@edited'),
            emails => $emails,
            first_names => $first_names,
            middle_names => $middle_names,
            preferred_names => $preferred_names,
            birthdates => $birthdates,
            memberships => $memberships,
            reserve_contact => $patron->findvalue('./reserveContactDefault/@contact'),
            reserve_contact_sms => $patron->findvalue('./reserveContactDefault/@sms'),
            overdue_contact => $patron->findvalue('./overdueContact/@contact'),
            overdue_contact_sms => $patron->findvalue('./overdueContact/@sms'),
            due_warning_contact => $patron->findvalue('./dueWarningContact/@contact'),
            due_warning_contact_sms => $patron->findvalue('./dueWarningContact/@sms'),
            internet => $u3714,
            birthdate => $u3710
        };
        
        my $lines;
        foreach my $address ($patron->findnodes('.//address')) {
            foreach my $add_line ($address->findnodes('./line')) {
                if (defined $lines) { $lines = $lines . '|' . $add_line->to_literal; }
                    else { $lines = $add_line->to_literal; }
                }
            push @user_addresses, {
                patron_barcode => $patron->findvalue('./@barcode'),
                patron_id => $patron->findvalue('./@id'),
                lines => $lines,
                locality => $address->findvalue('./@locality'),
                country_division => $address->findvalue('./@countryDivision'),
                country => $address->findvalue('./@country'),
                postal_code => $address->findvalue('./@postalCode'),
                mailing => $address->findvalue('./@mailing')
            };
        }

        foreach my $phone ($patron->findnodes('.//phone')) {
            push @user_phones, {
                patron_barcode => $patron->findvalue('./@barcode'),
                patron_id => $patron->findvalue('./@id'),
                number => $phone->findvalue('./@number'),
                area_code => $phone->findvalue('./@areaCode'),
                type => $phone->findvalue('./@type'),  #should be home/work/mobile/other/none
                phone_id => $phone->findvalue('./@id'),
                country_code => $phone->findvalue('./@countryCode')
            };
        }

        foreach my $note ($patron->findnodes('.//patronNote')) {
            my $message = $note->findvalue('./@message');
            $message =~ s/\\$//;
            push @user_notes, {
                patron_barcode => $patron->findvalue('./@barcode'),
                patron_id => $patron->findvalue('./@id'),
                active => $note->findvalue('./@active'),
                signature => $note->findvalue('./@signature'),
                urgent => $note->findvalue('./@urgent'),
                sensitive => $note->findvalue('./@sensitive'),
                date_added => $note->findvalue('./dateAdded'),
                date_cleared => $note->findvalue('./dateCleared'),
                last_updated => $note->findvalue('./dateUpdated'),
                message => $message
            };  
        }   
        
        foreach my $fine ($patron->findnodes('.//fine')) {
            push @patron_fines, {
                patron_barcode => $patron->findvalue('./@barcode'),
                patron_id => $patron->findvalue('./@id'),
                fine_id => $fine->findvalue('./@id'), 
                checkout => $fine->findvalue('./@checkout'),
                amount_paid_cents => $fine->findvalue('./@amountPaidCents'),
                status => $fine->findvalue('./@status'),
                updated => $fine->findvalue('./@updated'),
                amountCents=> $fine->findvalue('./@amountCents')
            };
        }

    }

    my $num_user_rows = @users;
    open my $out_user_fh, ">:utf8", $out_user_file or die "can not open $out_user_file file for writing.\n";
    print "writing $out_user_file ... \n";
    print $out_user_fh "patron_id\tpatron_barcode\tcreated\tusage_count\tfamily_id\texpiration\tedited\tlast_name\tfirst_name\tmiddle_name\tpreferred_name\tbirthdates\temails\tmemberships\treserve_contact\treserve_contact_sms\toverdue_contact\toverdue_contact_sms\tdue_warning_contact\tdue_warning_sms\tbirthdate\tinternet\n";
    foreach my $built_user_hash( @users ) {
        print $out_user_fh "$built_user_hash->{patron_id}\t";
        print $out_user_fh "$built_user_hash->{patron_barcode}\t";
        print $out_user_fh "$built_user_hash->{created}\t";
        print $out_user_fh "$built_user_hash->{usage_count}\t";
        print $out_user_fh "$built_user_hash->{family_id}\t";
        print $out_user_fh "$built_user_hash->{expiration}\t";
        print $out_user_fh "$built_user_hash->{edited}\t";
        print $out_user_fh "$built_user_hash->{last_name}\t";
        print $out_user_fh "$built_user_hash->{first_names}\t"; #plural names indicate arrays seperated by pipe
        print $out_user_fh "$built_user_hash->{middle_names}\t";
        print $out_user_fh "$built_user_hash->{preferred_names}\t";
        print $out_user_fh "$built_user_hash->{birthdates}\t";
        print $out_user_fh "$built_user_hash->{emails}\t";
        print $out_user_fh "$built_user_hash->{memberships}\t";
        print $out_user_fh "$built_user_hash->{reserve_contact}\t";
        print $out_user_fh "$built_user_hash->{reserve_contact_sms}\t";
        print $out_user_fh "$built_user_hash->{overdue_contact}\t";
        print $out_user_fh "$built_user_hash->{overdue_contact_sms}\t";
        print $out_user_fh "$built_user_hash->{due_warning_contact}\t";
        print $out_user_fh "$built_user_hash->{due_warning_contact_sms}\t";
        print $out_user_fh "$built_user_hash->{birthdate}\t";
        print $out_user_fh "$built_user_hash->{internet}\n";
    } 
    close $out_user_fh;
    print "$num_user_rows rows written plus headers to $out_user_file.\n";

    my $num_address_rows = @user_addresses;
    open my $out_address_fh, ">:utf8", $out_address_file or die "can not open $out_address_file file for writing.\n";
    print "writing $out_address_file ... \n";
    print $out_address_fh "patron_id\tpatron_barcode\tlines\tlocality\tcountry_division\tcountry\tpostal_code\tmailing\n";
    foreach my $built_address_hash( @user_addresses ) { 
        print $out_address_fh "$built_address_hash->{patron_id}\t";
        print $out_address_fh "$built_address_hash->{patron_barcode}\t";
        print $out_address_fh "$built_address_hash->{lines}\t";
        print $out_address_fh "$built_address_hash->{locality}\t";
        print $out_address_fh "$built_address_hash->{country_division}\t";
        print $out_address_fh "$built_address_hash->{country}\t";
        print $out_address_fh "$built_address_hash->{postal_code}\t";
        print $out_address_fh "$built_address_hash->{mailing}\n";
    }   
    close $out_address_fh;
    print "$num_address_rows rows written plus headers to $out_address_file.\n";

    my $num_phone_rows = @user_phones;
    open my $out_phone_fh, ">:utf8", $out_phone_file or die "can not open $out_phone_file file for writing.\n";
    print "writing $out_phone_file ... \n";
    print $out_phone_fh "patron_id\tpatron_barcode\tphone_id\tnumber\tarea_code\ttype\tcountry_code\n";
    foreach my $built_phone_hash( @user_phones ) {
        print $out_phone_fh "$built_phone_hash->{patron_id}\t";
        print $out_phone_fh "$built_phone_hash->{patron_barcode}\t";
        print $out_phone_fh "$built_phone_hash->{phone_id}\t";
        print $out_phone_fh "$built_phone_hash->{number}\t";
        print $out_phone_fh "$built_phone_hash->{area_code}\t";
        print $out_phone_fh "$built_phone_hash->{type}\t";
        print $out_phone_fh "$built_phone_hash->{country_code}\n";
    }
    close $out_phone_fh;
    print "$num_phone_rows rows written plus headers to $out_phone_file.\n";

    my $num_note_rows = @user_notes;
    open my $out_note_fh, ">:utf8", $out_note_file or die "can not open $out_note_file file for writing.\n";
    print "writing $out_note_file ... \n";
    print $out_note_fh "patron_id\tpatron_barcode\tactive\tsignature\turgent\tsensitive\tdate_added\tdate_cleared\tlast_updated\tmessage\n";
    foreach my $built_note_hash( @user_notes ) {
        print $out_note_fh "$built_note_hash->{patron_id}\t";
        print $out_note_fh "$built_note_hash->{patron_barcode}\t";
        print $out_note_fh "$built_note_hash->{active}\t";
        print $out_note_fh "$built_note_hash->{signature}\t";
        print $out_note_fh "$built_note_hash->{urgent}\t";
        print $out_note_fh "$built_note_hash->{sensitive}\t";
        print $out_note_fh "$built_note_hash->{date_added}\t";
        print $out_note_fh "$built_note_hash->{date_cleared}\t";
        print $out_note_fh "$built_note_hash->{last_updated}\t";
        print $out_note_fh "$built_note_hash->{message}\n";
    }
    close $out_note_fh;
    print "$num_note_rows rows written plus headers to $out_note_file.\n";

    my $num_fine_rows = @patron_fines;
    open my $out_fine_fh, ">:utf8", $out_fine_file or die "can not open $out_fine_file file for writing.\n";
    print "writing $out_fine_file ... \n";
    print $out_fine_fh "patron_id\tpatron_barcode\tfine_id\tcheckout\tamount_paid_cents\tstatus\tupdated\tamountCents\n";
    foreach my $built_fine_hash( @patron_fines ) {
        print $out_fine_fh "$built_fine_hash->{patron_id}\t";
        print $out_fine_fh "$built_fine_hash->{patron_barcode}\t";
        print $out_fine_fh "$built_fine_hash->{fine_id}\t";
        print $out_fine_fh "$built_fine_hash->{checkout}\t";
        print $out_fine_fh "$built_fine_hash->{amount_paid_cents}\t";
        print $out_fine_fh "$built_fine_hash->{status}\t";
        print $out_fine_fh "$built_fine_hash->{updated}\t";
        print $out_fine_fh "$built_fine_hash->{amountCents}\n";
    }
    close $out_fine_fh;
    print "$num_fine_rows rows written plus headers to $out_fine_file.\n";
}

sub biblio_record_entry {
    my $dom = shift;
    my $outfile = shift;

    open my $out_fh, ">:utf8", $outfile or die "can not open $outfile file for writing.\n";
    my $num_rows = 0;
    print "Parsing for bib records...\n";
    print $out_fh "id\tusage_count\tstatus\tadded\tedited\tmarc\n"; 
    foreach my $bib ($dom->findnodes('///biblio')) {
        my $id = $bib->findvalue('./@id');
        my $usageCount = $bib->findvalue('./@usageCount');
        my $status = $bib->findvalue('./@status');
        my $added = $bib->findvalue('./@added');
        my $edited = $bib->findvalue('./@edited');
        my $marc = $bib->toString;
        $marc =~ s/<marc:/</g;
        $marc =~ s/<\/marc:/<\//g;
        $marc =~ s/<bib.+?>//g;
        $marc =~ s/<\/bib.+?>//g;
        $marc =~ s/\R//g;
        $marc =~ s/\s+</</g;
        $marc =~ s/>\s+/>/g;
        print $out_fh "$id\t";
        print $out_fh "$usageCount\t";
        print $out_fh "$status\t";
        print $out_fh "$added\t";
        print $out_fh "$edited\t";
        print $out_fh "$marc\n"; 
        $num_rows++;
    }
    close $out_fh;
    print "$num_rows rows written plus headers.\n";
}

sub abort {
    my $msg = shift;
    print STDERR "$0: $msg", "\n";
    print_usage();
    exit 1;
}

   
sub print_usage {
    print <<_USAGE_;

LDIF Exporter loads an ldif xml file version 032 into staging tables 
to load into Evergreen.  This implementation assumes that the user 
is using a distinct Postgres schema to stage tables from.  The Exporter
creates linked tables in the schema to production tables that inherit 
sequences and then child tables with data to manipulate.  

For example: patron data will be loaded in m_foo.actor_usr which inherits
from actor.usr and a m_foo.actor_usr_legacy is created.  The table is 
prepopulated with likely data but legacy value is suppplied in the legacy 
table in l_ so it can be non-destrutively manipulated.

LDIF Exporter requires the following parameter:   

  --mig_schema - the staging schema of the migration

Optionally you may also provide a file with --ldif_xml 

If --ldif_xml is not used then it will assume there is an export.xml file and
fail if not found.



_USAGE_
}

