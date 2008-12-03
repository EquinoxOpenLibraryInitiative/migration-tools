#!/usr/bin/perl
use open ':utf8';
use MARC::Batch;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );
use MARC::Field;
use Unicode::Normalize;

my $filetype = $ARGV[0]; # XML or USMARC
my $filename = $ARGV[1]; 
if (! $filetype && ! $filename ) { die "./script <USMARC or XML> <filename>\n"; }

my $count = 0;

binmode(STDOUT, ':utf8');
binmode(STDIN, ':utf8');

print join("\t",
    "bib_id",
    "bib001",
    "material_number",
    "call_number",
    "copy_location",
    "price_and_vendor",
    "subf_b",
    "subf_x",
    "bib961u"
) . "\n";


print STDERR "Processing " . $ARGV[$argnum] . "\n";

my $M;
open $M, '<:utf8', $ARGV[1];
my $batch = MARC::Batch->new($ARGV[0],$M);

$batch->strict_off();
$batch->warnings_off();

while ( my $record = $batch->next() ) {

    $count++;

    print STDERR "WARNINGS: Record $count : " .  join(":",@warnings) . " : continuing...\n" if ( @warnings );
    my $my_903 = $record->field('903');
    my $my_903a = $my_903 ? $my_903->subfield('a') : ''; # target bib id's here
    my @my_961_tags = $record->field('961'); my $bib961u = '';
    foreach my $my_961 ( @my_961_tags ) {
        my @subfield_u = $my_961->subfield('u');
        foreach my $u ( @subfield_u ) {
            $bib961u .= $u . "|";
        } 
    }
    $bib961u =~ s/\|$//;
    my @tags = $record->field('852');
    foreach my $tag ( @tags ) {
        if ($tag->subfield('p')) { # if material_number
                print join("\t",
                    $my_903a, # bib id
                    $record->field('001') ? $record->field('001')->as_string() : '', #bib001
                    $tag->subfield('p') || '', # material_number
                    $tag->subfield('h') || '', # call_number
                    $tag->subfield('c') || '', # copy_location
                    $tag->subfield('9') || '', # price_and_vendor
                    $tag->subfield('b') || '', # subf_b
                    $tag->subfield('x') || '', # subf_x
                    $bib961u # bib961u
                ) . "\n";
        }
    }

}
print STDERR "Processed $count records\n";
