#!perl -T

use Test::More tests => 1;

BEGIN {
	use_ok( 'Equinox::Migration' );
}

diag( "Testing Equinox::Migration $Equinox::Migration::VERSION, Perl $], $^X" );
