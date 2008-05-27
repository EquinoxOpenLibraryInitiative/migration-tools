#!/usr/bin/perl
use DBI;
use Data::Dumper;

################ THIS RESOURCE IS FOR PINES PRODUCTION
my $SOURCE_DBI_RESOURCE = "dbi:Pg:dbname=sparkle;host=10.1.0.12;port=5432";
my $SOURCE_DBI_USER = 'postgres';
my $SOURCE_DBI_PASSWD = '';
my $source_dbh = DBI->connect($SOURCE_DBI_RESOURCE, $SOURCE_DBI_USER, $SOURCE_DBI_PASSWD) or die("Database error: $DBI::errstr");
my $primary_fingerprint_tablename = "public.quitman_full_fingerprint_set";

sub fetch_record {

    my $item_form = shift;
    my $date1 = shift;
    my $record_type = shift;
    my $bib_lvl = shift;
    my $title = shift;
    my $sql = "select id from $primary_fingerprint_tablename where " . join(' AND ',
        " item_form = ".$source_dbh->quote($item_form),
        " substring = ".$source_dbh->quote($date1),
        " item_type = ".$source_dbh->quote($record_type),
        " bib_level = ".$source_dbh->quote($bib_lvl),
        " title = ".$source_dbh->quote($title),
    );
    my $source_sth = $source_dbh->prepare($sql) or die("prepare error: $DBI::errstr \n[$sql]");
    $source_sth->execute() or die("execute error: $DBI::errstr \n[$sql]");

    while ( my ($id) = $source_sth->fetchrow_array ) {

        print "$id\n";

    }
    $source_sth->finish();


}

while (my $line = <>) {
    chomp $line;
    my ($id,$item_form,$date1,$record_type,$bib_lvl,$title) = split(/\t/,$line);
    if ($id eq 'id') { next; }
    fetch_record($item_form,$date1,$record_type,$bib_lvl,$title);
}

$source_dbh->disconnect;

