#!/usr/bin/perl

# USAGE: ./opac_settings.pl --bootstrap /openils/conf/opensrf_core.xml --schema m_sage
# Produces output that can be pasted into /etc/apache2/sites-available/eg.conf

use strict;
use DBI;
use FileHandle;
use Getopt::Long;
use OpenSRF::EX qw/:try/;
use OpenSRF::Utils::Logger qw/$logger/;
use OpenSRF::System;
use OpenSRF::AppSession;
use OpenSRF::Utils::SettingsClient;

use Data::Dumper;

use open ':utf8';

binmode(STDIN, ':utf8');
binmode(STDOUT, ':utf8');

$| = 1;

my ($config, $schema);

GetOptions(
	"bootstrap=s"	=> \$config,
	"schema=s" => \$schema,
);

OpenSRF::System->bootstrap_client( config_file => $config );

# XXX Get this stuff from the settings server
my $sc = OpenSRF::Utils::SettingsClient->new;
my $db_driver = $sc->config_value( reporter => setup => database => 'driver' );
my $db_host = $sc->config_value( reporter => setup => database => 'host' );
my $db_port = $sc->config_value( reporter => setup => database => 'port' );
my $db_name = $sc->config_value( reporter => setup => database => 'db' );
if (!$db_name) {
    $db_name = $sc->config_value( reporter => setup => database => 'name' );
    print STDERR "WARN: <database><name> is a deprecated setting for database name. For future compatibility, you should use <database><db> instead." if $db_name; 
}
my $db_user = $sc->config_value( reporter => setup => database => 'user' );
my $db_pw = $sc->config_value( reporter => setup => database => 'pw' );

die "Unable to retrieve database connection information from the settings server" unless ($db_driver && $db_host && $db_port && $db_name && $db_user);

my $dsn = "dbi:" . $db_driver . ":dbname=" . $db_name .';host=' . $db_host . ';port=' . $db_port;

my $dbh = DBI->connect($dsn,$db_user,$db_pw, {AutoCommit => 1, pg_enable_utf8 => 1, RaiseError => 1});

my $SQL = 'SELECT DISTINCT ou.id, ou.name FROM actor.org_unit ou, '.$schema.'.opac_settings os WHERE os.org_unit = ou.id ORDER BY ou.name';

my $ous = $dbh->selectcol_arrayref($SQL);

print "NameVirtualHost *:80\n";
print "NameVirtualHost *:443\n\n";

for my $ou_id ( @$ous ) {

  $SQL = "SELECT * FROM actor.org_unit WHERE id = $ou_id";
  my $ou = $dbh->selectrow_hashref( $SQL, {} );
  
  $SQL = "SELECT * FROM ".$schema.".opac_settings WHERE org_unit = $ou_id";
  my $settings = $dbh->selectall_hashref( $SQL, 'id' );

  my %s = {};
 
  foreach my $key (keys %$settings) {
    $$settings{$key}->{value} =~ s/^"//;
    $$settings{$key}->{value} =~ s/"$//;
    if ($$settings{$key}->{name} eq 'opac.server_alias') {
      push @{$s{'opac.server_alias'}}, $$settings{$key}->{value};
    } else {
      $s{$$settings{$key}->{name}} = $$settings{$key}->{value};
    } 
  }

  print "# " . "-"x70 . "\n";
  print "# $$ou{name} (shortname '$$ou{shortname}', id $$ou{id})\n";
  print "# " . "-"x70 . "\n";

  print "\n";
  print "<VirtualHost *:80>\n";
  print "  ServerName $s{'opac.server_name'}\n";
  foreach my $alias (@{$s{'opac.server_alias'}}) { print "  ServerAlias $alias\n"; }
  print "  DocumentRoot /openils/var/web/\n";  
  print "  DirectoryIndex index.xml index.html index.xhtml\n";
  print "  RedirectMatch ^/\$ http://$s{'opac.server_name'}/opac/en-US/skin/$s{'opac.directory'}/xml/index.xml?ol=$ou_id\n";
  print "  RedirectMatch default/css/colors.css\$ http://$s{'opac.server_name'}/opac/theme/$s{'opac.directory'}/css/colors.css\n";
  print "  RedirectMatch ^/favicon.ico\$ http://$s{'opac.server_name'}/opac/images/$s{'opac.directory'}/favicon.ico\n";
  print "  RedirectMatch images/main_logo.jpg\$ http://$s{'opac.server_name'}/opac/images/$s{'opac.directory'}/main_logo.jpg\n";
  print "  RedirectMatch images/small_logo.jpg\$ http://$s{'opac.server_name'}/opac/images/$s{'opac.directory'}/small_logo.jpg\n";
  print "  RedirectMatch en-US/extras/slimpac/(start|advanced).html http://$s{'opac.server_name'}/opac/en-US/extras/slimpac/\$1.html\n";
  print "  Include eg_vhost.conf\n";
  print "</VirtualHost>\n";

  print "\n";
  print "<VirtualHost *:443>\n";
  print "  ServerName $s{'opac.server_name'}\n";
  foreach my $alias (@{$s{'opac.server_alias'}}) { print "  ServerAlias $alias\n"; }
  print "  DocumentRoot /openils/var/web/\n";  
  print "  DirectoryIndex index.xml index.html index.xhtml\n";
  print "  RedirectMatch ^/\$ https://$s{'opac.server_name'}/opac/en-US/skin/$s{'opac.directory'}/xml/index.xml?ol=$ou_id\n";
  print "  RedirectMatch default/css/colors.css\$ https://$s{'opac.server_name'}/opac/theme/$s{'opac.directory'}/css/colors.css\n";
  print "  RedirectMatch ^/favicon.ico\$ https://$s{'opac.server_name'}/opac/images/$s{'opac.directory'}/favicon.ico\n";
  print "  RedirectMatch images/main_logo.jpg\$ https://$s{'opac.server_name'}/opac/images/$s{'opac.directory'}/main_logo.jpg\n";
  print "  RedirectMatch images/small_logo.jpg\$ https://$s{'opac.server_name'}/opac/images/$s{'opac.directory'}/small_logo.jpg\n";
  print "  RedirectMatch en-US/extras/slimpac/(start|advanced).html https://$s{'opac.server_name'}/opac/en-US/extras/slimpac/\$1.html\n";
  print "  SSLEngine on\n";
  print "  SSLCipherSuite ALL:!ADH:!EXPORT56:RC4+RSA:+HIGH:+MEDIUM:+LOW:+SSLv2:+EXP:+eNULL\n";
  print "  SSLCertificateFile $s{'opac.ssl_cert'}\n";
  print "  SSLCertificateKeyFile $s{'opac.ssl_key'}\n";
  print "  SSLCertificateChainFile ssl/gd_bundle.crt\n";
  print "  Include eg_vhost.conf\n";
  print "  BrowserMatch \".*MSIE.*\" \\\n";
  print "    nokeepalive ssl-unclean-shutdown \\\n";
  print "    downgrade-1.0 force-response-1.0\n";
  print "</VirtualHost>\n";

  print "\n";

}
