#!/usr/bin/perl

# Copyright (c) 2020 Equinox Open Library Initiative
# Author: Rogan Hamby <rhamby@equinoxinitiative.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>

use strict;
use warnings;
use DBI;
use Getopt::Long;
use Text::CSV;
use Data::Dumper;
use List::MoreUtils qw(first_index);

my $db;
my $dbuser;
my $dbhost;
my $dbpw;
my $dbport = '5432';
my $file;
my $delimiter = ',';
my $debug = 0; 
my $matchpoint = 'usrname';
my $org_unit;
my $org_id;
my $date_format;
my $default_password;
my $ident_type = 3;
my $alert_message;
my $alert_title = 'Needs Staff Attention';
my $profile;
my $home_ou;
my $print_au_id = 0;
my $session = time();

my $ret = GetOptions(
    'db:s'              => \$db,
    'dbuser:s'          => \$dbuser,
    'dbhost:s'          => \$dbhost,
    'dbpw:s'            => \$dbpw,
    'dbport:s'          => \$dbport,
    'debug:i'           => \$debug,
    'print_au_id:i'     => \$print_au_id,
    'file:s'            => \$file,
    'delimiter:s'       => \$delimiter,
    'matchpoint:s'      => \$matchpoint,
    'date_format:s'     => \$date_format,
    'ident_type:s'      => \$ident_type,
    'profile:s'         => \$profile,
    'default_password:s' => \$default_password,
    'alert_message:s'   => \$alert_message, 
    'alert_title:s'     => \$alert_title,
    'home_ou:s'         => \$home_ou,
    'org_unit:s'        => \$org_unit
);

my $dbh = connect_db($db, $dbuser, $dbpw, $dbhost, $dbport);
my @results;
my $query;
db_schema_check($dbh);
db_table_check_header($dbh);
db_table_check_log($dbh);
db_table_check_mapping($dbh);
db_add_password_function($dbh);

open(my $fh, '<', $file) or abort("Could not open $file!");

if ($matchpoint ne 'usrname' and $matchpoint ne 'cardnumber') { abort('invalid matchpoint defined'); }

if (!defined $org_unit) { abort('no org_unit defined'); }
my $prepped_org_unit = sql_wrap_text($org_unit);
if ($debug == 0) { log_event($dbh,$session,"beginning load for $org_unit"); } 
    else { log_event($dbh,$session,"beginning load for $org_unit in debug mode"); }
@results = sql_return($dbh,"SELECT id FROM actor.org_unit WHERE shortname = $prepped_org_unit;");
if ($results[0]) { $org_id = $results[0]; } else { abort('invalid org unit'); } 

#technically the org_unit is just a mapping identifier but I want to make sure it's real just in case of future use
if (defined $org_unit and defined $org_id) {
    log_event($dbh,$session,"org id $org_id found");
} else {
    fail($dbh,$session,"session fail - org unit not defined or invalid");
}

# initialize a bunch of stuff rather than go back to the db over and over 
my @valid_orgs = sql_return($dbh,"SELECT id FROM actor.org_unit_descendants($org_id);");
my %original_pgt = hash_from_sql($dbh,"SELECT name, id FROM permission.grp_tree;");
my %mapped_pgt = hash_from_sql($dbh,"SELECT import_value, native_value FROM patron_loader.mapping WHERE mapping_type = 'profile' AND org_unit = $prepped_org_unit;");
my %original_libs = hash_from_sql($dbh,"SELECT shortname, id FROM actor.org_unit;");
my %mapped_libs = hash_from_sql($dbh,"SELECT import_value, native_value FROM patron_loader.mapping WHERE mapping_type = 'home_library' AND org_unit = $prepped_org_unit;");

#if profile is from command line go ahead and get id a single time or fail if it's not valid 
my $profile_id;
if ($profile) { 
    $profile_id = $original_pgt{$profile}; 
    if (!defined $profile_id) { abort("provided parameter profile is invalid"); }
}

#if home_ou is from command line ...
my $home_ou_id;
if ($home_ou) {
    $home_ou_id = $original_libs{$home_ou};
    if (!defined $home_ou_id) { abort("provided home ou parameter is invalid"); }
}

#some values, notably home_library and profile can be mapped to substitute values if the exporting 
#system can not supply evergreen native values
my @columns = ("cardnumber","profile","usrname","passwd","net_access_level",
"family_name","first_given_name","second_given_name","pref_first_given_name","name_keywords",
"email","home_library","day_phone","evening_phone","other_phone","dob","ident_type","passwd",
"active","barred","juvenile",
"add1_street1","add1_street2","add1_city","add1_county","add1_state","add1_country","add1_post_code",
"add2_street1","add2_street2","add2_city","add2_county","add2_state","add2_country","add2_post_code",
"statcat_name1","statcat_value1","statcat_name2","statcat_value2","statcat_name3","statcat_value3");
my %column_positions;
my %column_values;
foreach my $column (@columns) { $column_positions{$column} = -1; }
my $rawlines = 0;
my $i = 0;
my $skipped = 0;
my $msg;
my $csv = Text::CSV->new({ sep_char => $delimiter });

if ($alert_message) { 
    $alert_message = sql_wrap_text($alert_message); 
    $alert_title = sql_wrap_text($alert_title);
}

if ($debug == 1) { print "Debug flag is on ... no patrons will be added or updated.\n" }
if ($debug == 1) { print "---------------------------------------------------------\n" }

while (my $line = <$fh>) {
    $rawlines++;
    $line =~ s/\r//g; 
    if ($csv->parse($line)) {
        $i++;
        if ($debug != 0 and $i != 1) { print "========================= processing line $i\n"; }
        if ($i % 100 == 0) { print "Processing row $i\n"; }
        my @fields = $csv->fields();
        if ($i == 1) {  #get positions from default names first, then mapped ones
            while (my ($col,$pos) = each %column_positions) {
                $column_positions{$col} = first_index { lc($_) eq lc($col) } @fields;
            }
            #for consistency we should probably have these in a hash instead of looked up but we only do it once at the start so meh...
            while (my ($col,$pos) = each %column_positions) {
                if ($column_positions{$col} != -1) { next; }
                my $sql_col = sql_wrap_text($col);
                @results = sql_return($dbh,"SELECT import_header FROM patron_loader.header WHERE default_header = $sql_col;");
                if ($results[0]) { $column_positions{$col} = first_index { lc($_) eq lc($results[0]) } @fields; }
            }
            #no need to keep fields not in here so ... byebye
            while (my ($col,$pos) = each %column_positions) { if ($pos == -1) { delete $column_positions{$col}; } }
            #make sure required columns or parameters are present, fail if not 
            my $missing_columns = '';
            if (!defined $column_positions{'cardnumber'}) { $missing_columns = join('',$missing_columns,'cardnumber'); }
            if (!defined $column_positions{'usrname'}) { $missing_columns = join('',$missing_columns,'usrname'); }
			if ($missing_columns ne '') { fail($dbh,$session,"required column(s) are missing: $missing_columns"); }
            #now copy the hash structure for reading the data 
            while (my ($col,$pos) = each %column_positions) { $column_values{$col} = ''; }
        }  else { #actual data
            while (my ($col,$val) = each %column_values) { $column_values{$col} = $fields[$column_positions{$col}]; }
            if (!defined $column_values{'usrname'} or !defined $column_values{'cardnumber'} #make sure basic values are present, homelib and profile checked later
                or !defined $column_values{'family_name'} or !defined $column_values{'first_given_name'}
            ) {
                $skipped++;
                $msg = "required value in row with usrname $column_values{'usrname'} and cardnumber or $column_values{'cardnumber'} is null";
                log_event($dbh,$session,$msg);
                if ($debug != 0) { print "$msg\n" }
                next;
            }
            if ($column_values{'dob'}) { $column_values{'dob'} = sql_date($dbh,$column_values{'dob'},$date_format); }
            my $prepped_cardnumber = sql_wrap_text($column_values{'cardnumber'});
            my $prepped_usrname = sql_wrap_text($column_values{'usrname'});
            my $prepped_profile_id = get_original_id(\%original_pgt,\%mapped_pgt,$column_values{'profile'},$profile_id);
            my $prepped_home_ou_id = get_original_id(\%original_libs,\%mapped_libs,$column_values{'home_library'},$home_ou_id);
            if (!defined $prepped_home_ou_id or !defined $prepped_profile_id) { 
                $skipped++;
                $msg = "could not find valid home library or profile id (or both) for $column_values{'cardnumber'}";
                log_event($dbh,$session,$msg);
                if ($debug != 0) { print "$msg\n" }
                next;
            } 
            if ($matchpoint eq 'usrname') {
                $query = "SELECT id FROM actor.usr WHERE usrname = $prepped_usrname;";
            } else {
                $query = "SELECT usr FROM actor.card WHERE barcode = $prepped_cardnumber;";
            }
            @results = sql_return($dbh,$query);
            my $au_id = $results[0];
            #standardize boolean t/f true/false to TRUE/FALSE 
            if ($column_values{'active'}) { $column_values{'active'} = sql_boolean($column_values{'active'}); }
            if ($column_values{'barred'}) { $column_values{'barred'} = sql_boolean($column_values{'barred'}); }
            if ($column_values{'juvenile'}) { $column_values{'juvenile'} = sql_boolean($column_values{'juvenile'}); }
            #since usrname and barcode both need to be unique having a valid au_id alone isn't enough, we need to test 
            #0 == match found for another au_id, 1 == found for this au_id, 2 == not found 
            my $valid_barcode = check_barcode($dbh,$au_id,$prepped_cardnumber);
            my $valid_usrname = check_usrname($dbh,$au_id,$prepped_usrname);
            #we don't need the sql friendly strings separate from the hash anymore so put the calculated ones into hash
            $column_values{'home_library'} = $prepped_home_ou_id;
            $column_values{'profile'} = $prepped_profile_id;
            if ($valid_barcode == 0 or $valid_usrname == 0) {
                $skipped++;
                $msg = "usrname $column_values{'usrname'} or cardnumber $column_values{'$cardnumber'} found with other user account";
                log_event($dbh,$session,$msg);
                if ($debug != 0) { print "$msg\n" }
                next;
            }
            my $update_usr_str;
            my $insert_usr_str;
            if ($au_id) { #update record
                #$valid_barcode has to be 1 or 2 now so ..... 
                if ($valid_barcode == 1) { 
                    $query = "UPDATE actor.card SET active = TRUE WHERE barcode = $prepped_cardnumber;";
                    sql_no_return($dbh,$query,$debug);
                } else { 
                    $query = "INSERT INTO actor.card (usr,barcode) VALUES ($au_id,$prepped_cardnumber);";
                    sql_no_return($dbh,$query,$debug); 
                }
                if (!defined $column_positions{'family_name'} 
                    or !defined $column_positions{'first_given_name'}
                    or !defined $column_values{'home_library'}
                    or !defined $column_values{'profile'}
                    or !defined $column_values{'passwd'}
                ) { 
                    $skipped++;
                    $msg = "usrname $column_values{'usrname'} or cardnumber $column_values{'$cardnumber'} insert failed";
                    log_event($dbh,$session,$msg);
                    if ($debug != 0) { print "$msg\n" }
                }    
                $update_usr_str = update_au_sql($au_id,%column_values);
                sql_no_return($dbh,$update_usr_str,$debug); 
            } else {  #create record
                $insert_usr_str = insert_au_sql($dbh,%column_values);
                sql_no_return($dbh,$insert_usr_str,$debug); 
                @results = sql_return($dbh,"SELECT id FROM actor.usr WHERE usrname = $prepped_usrname;");
                if ($debug == 0) { $au_id = $results[0]; } else { $au_id = 'debug'; }
                #if here the card number shouldn't be in use so we have to make it 
                $query = "INSERT INTO actor.card (usr,barcode) VALUES ($au_id,$prepped_cardnumber);";
                sql_no_return($dbh,$query,$debug); 
            }
            $query = "SELECT id FROM actor.card WHERE barcode = $prepped_cardnumber;";
            if ($debug == 0) { 
                @results = sql_return($dbh,"SELECT id FROM actor.card WHERE barcode = $prepped_cardnumber;");
            } else {
                print "$query\n";
            }
            my $acard_id;
            if ($debug == 0) { $acard_id = $results[0]; } else { $acard_id = 'debug'; }
            $query = "UPDATE actor.usr SET card = $acard_id WHERE id = $au_id;";
            sql_no_return($dbh,$query,$debug); 
            #make sure password is salted and all that
            my $prepped_password = sql_wrap_text($column_values{'passwd'}); 
            $query = "SELECT * FROM patron_loader.set_salted_passwd($au_id,$prepped_password);";
            sql_no_return($dbh,$query,$debug); 
            if ($alert_message) {
                $query = "INSERT INTO actor.usr_message (usr,title,message,sending_lib) VALUES ($au_id,$alert_title,$alert_message,$org_id);";
                sql_no_return($dbh,$query,$debug); 
            }
            #address fun, first if either address exists and then don't assume just b/c there is an add2 there is an add1
            if ($column_values{add1_street1} or $column_values{add2_street1}) { 
                $query = "UPDATE actor.usr SET mailing_address = NULL WHERE id = $au_id;";
                sql_no_return($dbh,$query,$debug); 
                $query = "DELETE FROM actor.usr_address WHERE usr = $au_id AND address_type = 'MAILING';";
                sql_no_return($dbh,$query,$debug); 
            }
            if ($column_values{add2_street1}) {
                $query = insert_addr_sql($au_id,2,%column_values);
                sql_no_return($dbh,$query,$debug);
            }
            if ($column_values{add1_street1}) {
                $query = insert_addr_sql($au_id,1,%column_values); 
                sql_no_return($dbh,$query,$debug);
            }
            if ($column_values{add1_street1} or $column_values{add2_street1}) {
                $query = "WITH x AS (SELECT MAX(id) AS id, usr FROM actor.usr_address WHERE usr = $au_id GROUP BY 2) UPDATE actor.usr au SET mailing_address = x.id FROM x WHERE x.usr = au.id;";
                sql_no_return($dbh,$query,$debug);
            }
        if ($print_au_id != 0) { print "$au_id\n"; }
        }
    }
}
close($fh);
log_event($dbh,$session,"raw lines in file",$rawlines);
log_event($dbh,$session,"rows processed",$i-1);
log_event($dbh,$session,"rows skipped",$skipped);
log_event($dbh,$session,"session closing normally");
my $j = $i -1;
print "========================= we are done!\n";
print "$rawlines raw lines in file\n";
print "$j rows processed not including header\n";
print "$skipped rows skipped\n";

$dbh->disconnect();

########### end of main body, start of functions

sub abort {
    my $msg = shift;
    print STDERR "$0: $msg", "\n";
    exit 1;
}

sub check_barcode {
    my ($dbh,$au_id,$barcode) = @_;
    my @results = sql_return($dbh,"SELECT usr FROM actor.card WHERE barcode = $barcode;");
    if (!defined $results[0]) { return 2; }
    if ($results[0] == $au_id) { return 1; }
    return 0;
}

sub check_usrname {
    my ($dbh,$au_id,$usrname) = @_;
    my @results = sql_return($dbh,"SELECT id FROM actor.usr WHERE usrname = $usrname;");
    if (!defined $results[0]) { return 2; }
    if ($results[0] == $au_id) { return 1; }
    return 0;
}

sub connect_db {
    my ($db, $dbuser, $dbpw, $dbhost, $dbport) = @_;

    my $dsn = "dbi:Pg:host=$dbhost;dbname=$db;port=$dbport";

    my $attrs = {
        ShowErrorStatement => 1,
        RaiseError => 1,
        PrintError => 1,
        pg_enable_utf8 => 1,
    };
    my $dbh = DBI->connect($dsn, $dbuser, $dbpw, $attrs);

    return $dbh;
}

sub db_add_password_function {
    my $dbh = shift;
    #this is functionally the same as the function in lp1858833 but it's not merged yet at this point so ...
    my $query = '
        CREATE OR REPLACE FUNCTION patron_loader.set_salted_passwd(INTEGER,TEXT) RETURNS BOOLEAN AS $$
            DECLARE
                usr_id              ALIAS FOR $1;
                plain_passwd        ALIAS FOR $2;
                plain_salt          TEXT;
                md5_passwd          TEXT;
            BEGIN
                SELECT actor.create_salt(\'main\') INTO plain_salt;
                SELECT MD5(plain_passwd) INTO md5_passwd;
                PERFORM actor.set_passwd(usr_id, \'main\', MD5(plain_salt || md5_passwd), plain_salt);
                RETURN TRUE;
            END;
        $$ LANGUAGE PLPGSQL STRICT VOLATILE;';
    sql_no_return($dbh,$query,0);
    return;
}

sub db_schema_check {
    my $dbh = shift;
    my $query = 'SELECT 1 FROM information_schema.schemata WHERE schema_name = \'patron_loader\';';
    my @results = sql_return($dbh,$query);
    if ($results[0]) { return; }
    $query = 'CREATE SCHEMA patron_loader;';
    sql_no_return($dbh,$query,0);
    return;
}

sub db_table_check_header {
    my $dbh = shift;
    my $query = 'SELECT 1 FROM information_schema.tables WHERE table_schema = \'patron_loader\' AND table_name = \'header\';';
    my @results = sql_return($dbh,$query);
    if ($results[0]) { return; }
    $query = 'CREATE TABLE patron_loader.header (id SERIAL, org_unit TEXT, import_header TEXT, default_header TEXT);';
    sql_no_return($dbh,$query,0);
    return;
}

sub db_table_check_log {
    my $dbh = shift;
    my $query = 'SELECT 1 FROM information_schema.tables WHERE table_schema = \'patron_loader\' AND table_name = \'log\';';
    my @results = sql_return($dbh,$query);
    if ($results[0]) { return; }
    $query = 'CREATE TABLE patron_loader.log (id SERIAL, session BIGINT, event TEXT, record_count INTEGER, logtime TIMESTAMP DEFAULT NOW());';
    sql_no_return($dbh,$query,0);
    return;
}

sub db_table_check_mapping {
    my $dbh = shift;
    my $query = 'SELECT 1 FROM information_schema.tables WHERE table_schema = \'patron_loader\' AND table_name = \'mapping\';';
    my @results = sql_return($dbh,$query);
    if ($results[0]) { return; }
    $query = 'CREATE TABLE patron_loader.mapping (id SERIAL, org_unit TEXT, mapping_type TEXT, import_value TEXT, native_value TEXT);';
    sql_no_return($dbh,$query,0);
    return;
}

sub fail {
    my ($dbh,$session,$failure) = @_;
    log_event($dbh,$session,$failure); 
    abort($failure);
}

sub get_original_id {
    my ($original,$mapped,$str,$default_id) = @_;
    my $mapped_value;
    if (%$original{$str}) { return %$original{$str}; }
    else {
        $mapped_value = %$mapped{$str};
        if ($mapped_value) { return %$original{$mapped_value}; }
    }
    if ($default_id) { return $default_id; } 
        else { return; }
}

sub hash_from_sql { 
    my ($dbh,$query) = @_;
    my %return_hash;
    my $sth = $dbh->prepare($query);
    $sth->execute();
    while (my @row = $sth->fetchrow_array) {
        $return_hash{$row[0]} = $row[1];
    }
    return %return_hash;
}

sub insert_addr_sql {
    my ($au_id,$x,%column_values) = @_;
    my $street1 = sql_wrap_text($column_values{join('','add',$x,'_street1')});
    my $street2 = sql_wrap_empty_text($column_values{join('','add',$x,'_street2')} // '');
    my $city = sql_wrap_empty_text($column_values{join('','add',$x,'_city')} // '');
    my $county = sql_wrap_empty_text($column_values{join('','add',$x,'_county')} // '');
    my $state = sql_wrap_empty_text($column_values{join('','add',$x,'_state')} // '');
    my $country = sql_wrap_empty_text($column_values{join('','add',$x,'_country')} // '');
    my $post_code = sql_wrap_empty_text($column_values{join('','add',$x,'_post_code')} // '');
	my $query;
    if ($street1) { $query = "INSERT INTO actor.usr_address (usr,street1,street2,city,county,state,country,post_code) VALUES ($au_id,$street1,$street2,$city,$county,$state,$country,$post_code);"; }
    return $query;
}

sub insert_au_sql {
    my ($au_id,%column_values) = @_;
    my $start = 'INSERT INTO actor.usr (';
    my $col_str;
    my $middle = ') VALUES (';
    my $val_str;
    my $end = ");";
    my @insert_columns;
    my @insert_values;
    #wrap strings but skip calculated ones and booleans 
    while (my ($col,$val) = each %column_values) {
        if (!defined $val) { next; }
        if ($col =~ m/add1/ or $col =~ m/add2/ or $col =~ m/stat/ or $col eq 'cardnumber') { next; } #skip columns not in actor.usr itself
        my $dontwrap = 0;
        if ($val eq 'TRUE' or $val eq 'FALSE') { $dontwrap = 1; }
        if ($col eq 'home_library' or $col eq 'profile' or $col eq 'ident_type') { $dontwrap = 1; }
        if ($dontwrap == 0) { $val = sql_wrap_text($val); }
        if ($col eq 'home_library') { $col = 'home_ou'; }
        push @insert_columns, $col;
        push @insert_values, $val;
    }
    #ident_type is required for actor.usr but not in file b/c it'll be rare to have so ... special handling here 
    if (!defined $column_values{'ident_type'}) {
        push @insert_columns, 'ident_type';
        push @insert_values, 3;
    }
    foreach my $ic (@insert_columns) { 
        if ($col_str) { $col_str = join(',',$col_str,$ic); } else { $col_str = $ic; }
    }
    foreach my $iv (@insert_values) {
        if ($val_str) { $val_str = join(',',$val_str,$iv); } else { $val_str = $iv; }
    }
   
    my $query = join('',$start,$col_str,$middle,$val_str,$end);
    return $query;
}

sub log_event {
    my ($dbh,$session,$event,$record_count) = @_;
    $event = sql_wrap_text($event);
    if (!defined $record_count) { $record_count = 0; }
    my $sql = "INSERT INTO patron_loader.log (session,event,record_count) VALUES ($session,$event,$record_count);";
    sql_no_return($dbh,$sql,0);
}

sub sql_boolean {
    my $str = shift;
    $str = lc($str);
    $str =~ s/^\s+|\s+$//g;
    my $value;
    if ($str eq 't' or $str eq 'true') { $value = 'TRUE'; }
    if ($str eq 'f' or $str eq 'false') { $value = 'FALSE'; }
    return $value;
}

sub sql_date {
    my ($dbh,$date,$date_format) = @_;
    if (!defined $date_format) { $date_format = 'YYYY/MM/DD'; }
    $date = sql_wrap_text($date);
    $date_format = sql_wrap_text($date_format);
    my $query = "SELECT TO_DATE($date,$date_format);";
    my @results = sql_return($dbh,$query);
    return $results[0];
}

sub sql_no_return {
    my $dbh = shift;
    my $statement = shift;
	my $debug = shift;
    my $sth;
    if ($debug == 0) {
        eval {
            $sth = $dbh->prepare($statement);
            $sth->execute();
        }
    } else {
        print "$query\n";
    }
    if ($@) { 
        $query =~ s/'//g;
        log_event($dbh,$session,"failed statement $query",0); 
    }
    return;
}

sub sql_return {
    my $dbh = shift;
    my $query = shift;
	my $debug = shift;
    my @results;
    my $sth = $dbh->prepare($query);
    $sth->execute();
    while (my @row = $sth->fetchrow_array) { push @results, @row; }
    return @results;
}

sub sql_wrap_empty_text {
    my $str = shift;
	$str = sql_wrap_text($str);
    if ($str eq 'NULL') { $str = "''"; }
    return $str;
}

sub sql_wrap_text {
    my $str = shift;
    $str =~ s/^\s+|\s+$//g;
    if ($str) { $str = '\'' . $str . '\''; } else { $str = 'NULL'; }
    return $str;
}

sub update_au_sql {
    my ($au_id,%column_values) = @_;
    my $start = 'UPDATE actor.usr SET ';
    my $middle;
    my $end = " WHERE id = $au_id;";
    #wrap strings but skip calculated ones and booleans 
    while (my ($col,$val) = each %column_values) {
        if (!defined $val) { next; }
        if ($col =~ m/add1/ or $col =~ m/add2/ or $col =~ m/stat/ or $col eq 'cardnumber') { next; } #skip columns not in actor.usr itself
        my $dontwrap = 0;
        if ($val eq 'TRUE' or $val eq 'FALSE') { $dontwrap = 1; }
        if ($col eq 'home_library' or $col eq 'profile' or $col eq 'ident_type') { $dontwrap = 1; }
        if ($dontwrap == 0) { $val = sql_wrap_text($val); }
        if ($col eq 'home_library') { $col = 'home_ou'; }  
        if (!defined $middle) { $middle = "$col = $val"; } else { $middle = join(', ', $middle, "$col = $val"); }
    }
    my $query = join('',$start,$middle,$end);
    return $query;
}

