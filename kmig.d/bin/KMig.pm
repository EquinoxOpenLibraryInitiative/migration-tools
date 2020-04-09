package KMig;

use strict;
use Exporter;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);

$VERSION        = 1.00;
@ISA            = qw(Exporter);
@EXPORT         = ();
@EXPORT_OK      = qw();
%EXPORT_TAGS    = (
                     DEFAULT => []
);

use DBI;
use Env qw(
    HOME MYSQL_HOST MYSQL_TCP_PORT MYSQL_USER MYSQL_DATABASE MYSQL_PW
    MIGSCHEMA MIGBASEWORKDIR MIGBASEGITDIR MIGGITDIR MIGWORKDIR
);

sub db_connect {
    my $dbh;
    if ($MYSQL_HOST) {
        $dbh = DBI->connect(
         "dbi:mysql:host=$MYSQL_HOST;dbname=$MYSQL_DATABASE;port=$MYSQL_TCP_PORT"
        ,$MYSQL_USER
        ,$MYSQL_PW
        ) || die "Unable to connect to $MYSQL_HOST:$MYSQL_TCP_PORT:$MYSQL_DATABASE:$MYSQL_USER : $!\n";
    } else {
        $dbh = DBI->connect("dbi:Pg:dbname=$MYSQL_DATABASE", "", "") || die "Unable to connect to $MYSQL_DATABASE : $!\n";
    }
    return $dbh;
}

sub db_disconnect {
    my $dbh = shift;
    $dbh->disconnect;
}

sub sql {
    my $sql = shift;
    chomp $sql;
    $sql =~ s/\n//g;
    print "\n$sql\n";
    return $sql;
}

sub die_if_no_env_migschema {
    die "MIGSCHEMA environment variable not set.  See 'mig env help'\n"
        unless $MIGSCHEMA;
}

sub check_for_db_migschema {
    return 1; # the schema is the same as the database name, which is the same as the koha instance name, in most cases
}

sub check_db_migschema_for_migration_tables {
    my $found = check_db_migschema_for_specific_table('m_borrowers');
    if (!$found) {
        print "Missing migration tables (such as m_borrowers)\n";
    }
    return $found;
}

sub check_db_migschema_for_specific_table {
    my $table = shift;
    my $dbh = db_connect();
    my $sth = $dbh->prepare("
        SELECT EXISTS(
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = " . $dbh->quote( $MYSQL_DATABASE ) . "
            AND table_name = " . $dbh->quote( $table ) . "
        );"
    );
    my $rv = $sth->execute()
        || die "Error checking migration schema ($MIGSCHEMA) for table ($table): $!";
    my @cols = $sth->fetchrow_array;
    $sth->finish;
    my $found;
    if ($cols[0]) {
        $found = 1;
    } else {
        $found = 0;
    }
    db_disconnect($dbh);
    return $found;
}

sub check_for_migration_tools {
    my $dbh = db_connect();
    my $sth = $dbh->prepare("
        SELECT EXISTS(
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = " . $dbh->quote( $MYSQL_DATABASE ) . "
            AND table_name = " . $dbh->quote( 'mt_init' ) . "
        );"
    );
    my $rv = $sth->execute()
        || die "Error checking for migration_tools: $!";
    my @cols = $sth->fetchrow_array;
    $sth->finish;
    db_disconnect($dbh);
    return $cols[0];
}

sub die_if_no_migration_tools {
    if (check_for_migration_tools()) {
        print "Found migration_tools\n";
    } else {
        die "Missing migration_tools\n";
    }
}

sub check_for_mig_tracking_table {
    my $dbh = db_connect();
    my $sth = $dbh->prepare("
        SELECT EXISTS(
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = " . $dbh->quote( $MYSQL_DATABASE ) . "
            AND table_name = 'm_tracked_file'
        );"
    );
    my $rv = $sth->execute()
        || die "Error checking for table (m_tracked_file): $!";
    my @cols = $sth->fetchrow_array;
    $sth->finish;
    db_disconnect($dbh);
    return $cols[0];
}

sub die_if_mig_tracking_table_exists {
    if (check_for_mig_tracking_table()) {
        die "Table m_tracked_file already exists.  Bailing init...\n";
    }
}

sub die_if_mig_tracking_table_does_not_exist {
    if (!check_for_mig_tracking_table()) {
        die "Table m_tracked_file does not exist.  Bailing...\n";
    }
}

sub check_for_mig_column_tracking_table {
    my $dbh = db_connect();
    my $sth = $dbh->prepare("
        SELECT EXISTS(
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = " . $dbh->quote( $MYSQL_DATABASE ) . "
            AND table_name = 'm_tracked_column'
        );"
    );
    my $rv = $sth->execute()
        || die "Error checking for table (m_tracked_column): $!";
    my @cols = $sth->fetchrow_array;
    $sth->finish;
    db_disconnect($dbh);
    return $cols[0];
}

sub die_if_mig_column_tracking_table_exists {
    if (check_for_mig_column_tracking_table()) {
        die "Table m_tracked_column already exists.  Bailing init...\n";
    }
}

sub die_if_mig_column_tracking_table_does_not_exist {
    if (!check_for_mig_column_tracking_table()) {
        die "Table m_tracked_column does not exist.  Bailing...\n";
    }
}

sub check_for_tracked_file {
    my $file = shift;
    my $options = shift;
    if (! -e $file) {
        die "file not found: $file\n" unless $options && $options->{'allow_missing'};
    }
    my $dbh = db_connect();
    my $sth = $dbh->prepare("
        SELECT id
        FROM m_tracked_file
        WHERE base_filename = " . $dbh->quote( $file ) . ";"
    );
    my $rv = $sth->execute()
        || die "Error checking table (m_tracked_file) for base_filename ($file): $!";
    my @cols = $sth->fetchrow_array;
    $sth->finish;
    db_disconnect($dbh);
    return $cols[0];
}

sub check_for_tracked_column {
    my ($table,$column,$options) = (shift,shift,shift);
    my $dbh = db_connect();
    my $sth = $dbh->prepare("
        SELECT id
        FROM m_tracked_column
        WHERE staged_table = " . $dbh->quote( $table ) . "
        AND staged_column = " . $dbh->quote( $column ) . ";"
    );
    my $rv = $sth->execute()
        || die "Error checking table (m_tracked_column) for $table.$column: $!";
    my @cols = $sth->fetchrow_array;
    $sth->finish;
    db_disconnect($dbh);
    return $cols[0];
}

sub status_this_file {
    my $file = shift;
    my $dbh = db_connect();
    my $sth = $dbh->prepare("
        SELECT *
        FROM m_tracked_file
        WHERE base_filename = " . $dbh->quote( $file ) . ";"
    );
    my $rv = $sth->execute()
        || die "Error retrieving data from table (m_tracked_file) for base_filename ($file): $!";
    my $data = $sth->fetchrow_hashref;
    $sth->finish;
    db_disconnect($dbh);
    return $data;
}

sub status_this_column {
    my ($table,$column) = (shift,shift);
    my $dbh = db_connect();
    my $sth = $dbh->prepare("
        SELECT *
        FROM m_tracked_column
        WHERE staged_table = " . $dbh->quote( $table ) . "
        AND staged_column = " . $dbh->quote( $column ) . ";"
    );
    my $rv = $sth->execute()
        || die "Error checking table (m_tracked_column) for $table.$column: $!";
    my $data = $sth->fetchrow_hashref;
    $sth->finish;
    db_disconnect($dbh);
    return $data;
}

1;

