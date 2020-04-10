package Mig;

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
    HOME PGHOST PGPORT PGUSER PGDATABASE MIGSCHEMA
    MIGBASEWORKDIR MIGBASEGITDIR MIGGITDIR MIGWORKDIR
);

sub db_connect {
    my $dbh;
    if ($PGHOST) {
        $dbh = DBI->connect(
         "dbi:Pg:host=$PGHOST;dbname=$PGDATABASE;port=$PGPORT"
        ,$PGUSER
        ,undef
        ) || die "Unable to connect to $PGHOST:$PGPORT:$PGDATABASE:$PGUSER : $!\n";
    } else {
        $dbh = DBI->connect("dbi:Pg:dbname=$PGDATABASE", "", "") || die "Unable to connect to $PGDATABASE : $!\n";
    }
    $dbh->do("SET search_path TO $MIGSCHEMA, evergreen, pg_catalog, public");
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
    my $dbh = db_connect();
    my $sth = $dbh->prepare("
        SELECT EXISTS(
            SELECT 1
            FROM pg_namespace 
            WHERE nspname = ?
        );"
    );
    my $rv = $sth->execute($MIGSCHEMA)
        || die "Error checking for migration schema ($MIGSCHEMA): $!";
    my @cols = $sth->fetchrow_array;
    $sth->finish;
    my $found;
    if ($cols[0]) {
        print "Found migration schema ($MIGSCHEMA) at $PGHOST:$PGPORT:$PGDATABASE:$PGUSER\n";
        $found = 1;
    } else {
        print "Migration schema ($MIGSCHEMA) does not exist at $PGHOST:$PGPORT:$PGDATABASE:$PGUSER\n";
        $found = 0;
    }
    db_disconnect($dbh);
    return $found;
}

sub check_db_migschema_for_migration_tables {
    my $found = check_db_migschema_for_specific_table('asset_copy');
    if (!$found) {
        print "Missing migration tables (such as $MIGSCHEMA.asset_copy)\n";
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
            WHERE table_schema = " . $dbh->quote( $MIGSCHEMA ) . "
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
            FROM pg_namespace
            WHERE nspname = 'migration_tools'
        );"
    );
    my $rv = $sth->execute()
        || die "Error checking for migration_tools schema: $!";
    my @cols = $sth->fetchrow_array;
    $sth->finish;
    db_disconnect($dbh);
    return $cols[0];
}

sub die_if_no_migration_tools {
    if (check_for_migration_tools()) {
        print "Found migration_tools schema\n";
    } else {
        die "Missing migration_tools schema\n";
    }
}

sub check_for_mig_tracking_table {
    my $dbh = db_connect();
    my $sth = $dbh->prepare("
        SELECT EXISTS(
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = " . $dbh->quote( $MIGSCHEMA ) . "
            AND table_name = 'tracked_file'
        );"
    );
    my $rv = $sth->execute()
        || die "Error checking for table (tracked_file): $!";
    my @cols = $sth->fetchrow_array;
    $sth->finish;
    db_disconnect($dbh);
    return $cols[0];
}

sub die_if_mig_tracking_table_exists {
    if (check_for_mig_tracking_table()) {
        die "Table $MIGSCHEMA.tracked_file already exists.  Bailing init...\n";
    }
}

sub die_if_mig_tracking_table_does_not_exist {
    if (!check_for_mig_tracking_table()) {
        die "Table $MIGSCHEMA.tracked_file does not exist.  Bailing...\n";
    }
}

sub check_for_mig_column_tracking_table {
    my $dbh = db_connect();
    my $sth = $dbh->prepare("
        SELECT EXISTS(
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = " . $dbh->quote( $MIGSCHEMA ) . "
            AND table_name = 'tracked_column'
        );"
    );
    my $rv = $sth->execute()
        || die "Error checking for table (tracked_column): $!";
    my @cols = $sth->fetchrow_array;
    $sth->finish;
    db_disconnect($dbh);
    return $cols[0];
}

sub die_if_mig_column_tracking_table_exists {
    if (check_for_mig_column_tracking_table()) {
        die "Table $MIGSCHEMA.tracked_column already exists.  Bailing init...\n";
    }
}

sub die_if_mig_column_tracking_table_does_not_exist {
    if (!check_for_mig_column_tracking_table()) {
        die "Table $MIGSCHEMA.tracked_column does not exist.  Bailing...\n";
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
        FROM $MIGSCHEMA.tracked_file
        WHERE base_filename = " . $dbh->quote( $file ) . ";"
    );
    my $rv = $sth->execute()
        || die "Error checking table (tracked_file) for base_filename ($file): $!";
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
        FROM $MIGSCHEMA.tracked_column
        WHERE staged_table = " . $dbh->quote( $table ) . "
        AND staged_column = " . $dbh->quote( $column ) . ";"
    );
    my $rv = $sth->execute()
        || die "Error checking table (tracked_column) for $table.$column: $!";
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
        FROM $MIGSCHEMA.tracked_file
        WHERE base_filename = " . $dbh->quote( $file ) . ";"
    );
    my $rv = $sth->execute()
        || die "Error retrieving data from table (tracked_file) for base_filename ($file): $!";
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
        FROM $MIGSCHEMA.tracked_column
        WHERE staged_table = " . $dbh->quote( $table ) . "
        AND staged_column = " . $dbh->quote( $column ) . ";"
    );
    my $rv = $sth->execute()
        || die "Error checking table (tracked_column) for $table.$column: $!";
    my $data = $sth->fetchrow_hashref;
    $sth->finish;
    db_disconnect($dbh);
    return $data;
}

1;

