#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use Data::Dumper;
use Getopt::Long;
use List::MoreUtils qw(firstidx);

my $t_db = '';
my $t_dbuser = '';
my $t_dbpw = '';
my $tdbhost = '';
my $tdbh = connect_db($t_db, $t_dbuser, $t_dbpw, $tdbhost);

my $p_db = '';
my $p_dbuser = '';
my $p_dbpw = '';
my $pdbhost = '';
my $pdbh = connect_db($p_db, $p_dbuser, $p_dbpw, $pdbhost);

my $schema = '';
my @tables = qw(actor.org_unit actor.hours_of_operation actor.org_address actor.org_unit_setting);
my $orgs = "'ou_1','ou_2','ou_3'";

foreach my $table (@tables) {
	drop_table_pair($pdbh,$schema,$table);
    transfer_table($tdbh,$pdbh,$table,$schema,$orgs);
    set_table_values($pdbh,$table,$schema);
}


###### subroutines beyond here 
##############################

sub booleanify {
	my $str = shift;

	if ($str eq '1') { $str = 'TRUE'; }
		else { $str = 'FALSE'; }

	return $str;
}

sub connect_db {
    my ($db, $dbuser, $dbpw, $dbhost) = @_;

    my $dsn = "dbi:Pg:host=$dbhost;dbname=$db;port=5432";

    my $attrs = {
        ShowErrorStatement => 1,
        RaiseError => 1,
        PrintError => 1,
        pg_enable_utf8 => 1,
    };
    my $dbh = DBI->connect($dsn, $dbuser, $dbpw, $attrs);
    if(!$dbh) { abort('failed to connect to database.')  }
    return $dbh;
}

sub create_tables {
	my ($pdbh,$schema,$table,$headers,$typecolumns) = @_;

	my $base = $table;
    $base =~ s/\./_/;
    $base = 't_' . $base;
    my $base_full = $schema . '.' . $base;
    my $legacy_full = $base_full . '_legacy';
    my $column_count = @$headers;
    my @legacy_rows;
    while ($column_count > 0) {
        my $h = shift @$headers;
        my $t = shift @$typecolumns;
        my $combined = 'l_' . $h . ' ' . $t;
        push @legacy_rows, $combined;
        $column_count--;
    }
    my $legacy_rows_str = join(', ', @legacy_rows);

	my $basequery = qq{ 
        SELECT migration_tools.build_variant_staging_table(?,?,?);
    };
    my $bq = $pdbh->prepare($basequery);
    $bq->execute($schema,$table,$base);

    my $legacyquery = qq{ CREATE TABLE $legacy_full ($legacy_rows_str) INHERITS ($base_full); };
    my $lq = $pdbh->prepare($legacyquery);
    $lq->execute();
}

sub drop_table_pair {
	my ($dbh, $schema, $table) = @_;
    $table =~ s/\./_/;

    my $basetable   = $schema . '.' . 't_' . $table;
    my $legacytable = $schema . '.' . 't_' . $table . '_legacy';

    my $droplegacy = qq{ DROP TABLE IF EXISTS $legacytable; };
    my $dl = $dbh->prepare($droplegacy);
    $dl->execute();

	my $dropbase = qq{ DROP TABLE IF EXISTS $basetable; };
    my $db = $dbh->prepare($dropbase);
    $db->execute();
}

sub insert_data {
	my ($pdbh,$table,$schema,$insert_list,$data) = @_;

    my $base = $table;
    $base =~ s/\./_/;
    $base = 't_' . $base;
    my $base_full = $schema . '.' . $base;
    my $legacy_full = $base_full . '_legacy';

    my $insert = qq{ 
		INSERT INTO $legacy_full ($insert_list) VALUES ($data);
    };
    my $sth = $pdbh->prepare($insert);
    $sth->execute();
}

sub return_column_type {
	my ($dbh, $str, $colname) = @_;
	my $typeof;
	my ($schema,$table) = split /\./, $str;

    my $query = qq{ 
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = ? 
			AND table_name = ? 
			AND column_name = ?;
	};
    my $sth = $tdbh->prepare($query);
    $sth->execute($schema,$table,$colname);
	my $result = $sth->fetchrow_arrayref()->[0];
	return $result;
}

sub set_table_values {
	my ($dbh, $table, $schema) = @_;
    
    #get from information_schema the columns and sync l_ and  non-l_ versions
    #skipping FKs 
    #all FKs will be handled manually 
}

sub text_wrap {
	my $str = shift;
    $str =~ s/"/""/;
    $str =~ s/'/''/;
    $str = "'" . $str . "'";
	return $str;
}

sub ts_wrap {
    my $str = shift;
    $str = "'" . $str . "'";
    return $str;
}

sub transfer_table {
    my ($tdbh,$pdbh,$table,$schema,$orgs) = @_;
    my $query;

    if ($table eq 'actor.org_unit') {
    	$query = qq{ SELECT * FROM actor.org_unit WHERE shortname IN ($orgs); };
	};

    if ($table eq 'actor.org_address') {
        $query = qq{ SELECT * FROM actor.org_address WHERE org_unit IN 
			(SELECT id FROM actor.org_unit WHERE shortname IN ($orgs)); };
    };

    if ($table eq 'actor.org_unit_setting') {
        $query = qq{ SELECT * FROM actor.org_unit_setting WHERE org_unit IN 
            (SELECT id FROM actor.org_unit WHERE shortname IN ($orgs)); };
    };

    if ($table eq 'actor.hours_of_operation') {
        $query = qq{ SELECT * FROM actor.hours_of_operation WHERE id IN 
            (SELECT id FROM actor.org_unit WHERE shortname IN ($orgs)); };
    };

    my $sth = $tdbh->prepare($query);
    $sth->execute();

	my @headers = @{ $sth->{NAME_lc} }; # get headers of sql query column 
	my @textcolumns;
    my @booleancolumns;
    my @typecolumns;
    my @timestampcolumns;
    foreach (@headers) { # get data types of columns 
		my $columntype = return_column_type($tdbh,$table,$_);
		if ($columntype eq 'text') { push @textcolumns, $_; }
        if ($columntype eq 'boolean') { push @booleancolumns, $_; }
        if (index($columntype, 'time') != -1) { push @timestampcolumns, $_; }
        push @typecolumns, $columntype;
	}

    my @textPositions; # give positions to the text columns 
	for my $textcolumn (@textcolumns) {
		my $ind = firstidx { $_ eq $textcolumn } @headers;
        push @textPositions, $ind + 1;
	}

    my @booleanPositions; # give positions to the text columns 
    for my $booleancolumn (@booleancolumns) {
        my $ind = firstidx { $_ eq $booleancolumn } @headers;
        push @booleanPositions, $ind + 1;
    }

    my @timestampPositions; # give positions to the text columns 
    for my $tscolumn (@timestampcolumns) {
        my $ind = firstidx { $_ eq $tscolumn } @headers;
        push @timestampPositions, $ind + 1;
    }

    my @ct_copy_headers = @headers; #don't want to send originals, using here
    my @ct_copy_typecolumns = @typecolumns;
    create_tables($pdbh,$schema,$table,\@ct_copy_headers,\@ct_copy_typecolumns);

    my $column_count = @headers;
    my $insert_list = '';
    while ($column_count > 0) {
        my $h = shift @headers;
        $h = 'l_' . $h . ',';
        $insert_list = $insert_list . $h;
        $column_count--;
    }
    chop($insert_list); # remove comma at end

    while (my @row = $sth->fetchrow_array) {
        my $i = 0;
        foreach (@row) {
            $i++;
            if ( defined $_ and grep( /^$i$/, @textPositions) ) { $_ = text_wrap($_); }
            if ( defined $_ and grep( /^$i$/, @booleanPositions) ) { $_ = booleanify($_); }
            if ( defined $_ and grep( /^$i$/, @timestampPositions) ) { $_ = ts_wrap($_); }
            $_ = 'NULL' unless defined; 
        }
        my $data = join(',', @row);
        insert_data($pdbh,$table,$schema,$insert_list,$data);
    }
}

