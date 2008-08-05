#!/usr/bin/perl
use strict;
use warnings;
use open ':utf8';

use Getopt::Long;
use MARC::Batch;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );
use MARC::Field;
use Unicode::Normalize;

my $conf  = {}; # configuration hashref
my $marc  = {}; # MARC record hashref
my $count = 0;
initialyze($conf);

for my $file (@ARGV) {

    print STDERR "Processing $file\n";

    open my $M, '<:utf8', $file;

    my $batch = MARC::Batch->new('XML',$M);
    $batch->strict_off();
    $batch->warnings_off();

    while ( my $record = $batch->next ) {
        $count++;

        my $id = $record->field($conf->{tag});
        unless ($id) {
            print STDERR "ERROR: This record is missing a", $conf->{tag},
              "field.\n", $record->as_formatted(), "\n=====\n";
            next;
        }
        $id = $id->as_string($conf->{subfield});

        my $leader = $record->leader();
        my $record_type = substr($leader,6,1);
        my $bib_lvl = substr($leader,7,1);

        my $my_008 = $record->field('008');
        $my_008 = $my_008->as_string() if ($my_008);
        my $date1 = substr($my_008,7,4) if ($my_008);
        my $date2 = substr($my_008,11,4) if ($my_008);

        my $item_form;
        if ( $record_type =~ /[gkroef]/ ) { # MAP, VIS
            $item_form = substr($my_008,29,1) if ($my_008);
        } else {
            $item_form = substr($my_008,23,1) if ($my_008);
        }

        my $title = $record->field('245');
        $title = $title->subfield('a') if $title;

        my @isbns = ();
        my @isbns_020;
        if ($record->field('020')) { @isbns_020 = $record->field('020'); }
        foreach my $f ( @isbns_020 ) {
            if ($f->subfield('a')) {
                if ( $f->subfield('a')=~/(\S+)/ ) {
                    push @isbns, $1;
                }
            }
        }
        my @isbns_024;
        if ($record->field('024')) { @isbns_024 = $record->field('024'); }
        foreach my $f ( @isbns_024 ) {
            if ($f->subfield('a')) {
                if ( $f->subfield('a')=~/(\S+)/ ) {
                    push @isbns, $1;
                }
            }
        }

        my $issn = $record->field('022');
        $issn = $issn->subfield('a') if $issn;

        my $lccn = $record->field('010');
        $lccn = $lccn->subfield('a') if $lccn;

        my $author;
        if ($record->field('100'))
          { $author = $record->field('100')->subfield('a'); }
        unless ( $author ) {
            $author = $record->field('110')->subfield('a')
              if ($record->field('110'));
            $author = $record->field('111')->subfield('a')
              if ($record->field('111'));
        }

        my $desc = $record->field('300');
        $desc = $desc->subfield('a') if $desc;

        my $pages;
        $pages = $1 if (defined $desc and $desc =~ /(\d+)/);

        my $my_260 = $record->field('260');
        my $publisher = $my_260->subfield('b') if $my_260;
        my $pubyear = $my_260->subfield('c') if $my_260;
        if ( $pubyear ) {
            if ( $pubyear =~ /(\d\d\d\d)/ )
              { $pubyear = $1; }
            else
              { $pubyear = ''; }
        }

        my $edition = $record->field('250');
        $edition = $edition->subfield('a') if $edition;

        # NORMALIZE
        $record_type = 'a' if ($record_type eq ' ');
        if ($title) {
            $title = NFD($title); $title =~ s/[\x{80}-\x{ffff}]//go;
            $title = lc($title);
            $title =~ s/\W+$//go;
        }
        if ($author) {
            $author = NFD($author); $author =~ s/[\x{80}-\x{ffff}]//go;
            $author = lc($author);
            $author =~ s/\W+$//go;
            if ($author =~ /^(\w+)/) {
                $author = $1;
            }
        }
        if ($publisher) {
            $publisher = NFD($publisher); $publisher =~ s/[\x{80}-\x{ffff}]//go;
            $publisher = lc($publisher);
            $publisher =~ s/\W+$//go;
            if ($publisher =~ /^(\w+)/) {
                $publisher = $1;
            }
        }

        # SPIT OUT FINGERPRINTS FROM THE "LOIS ALGORITHM"
        # If we're not getting good matches, we may want to change this.
        # The same thing goes for some other fields.
        if ($item_form && ($date1 =~ /\d\d\d\d/)
            && $record_type && $bib_lvl && $title) {
            if ($conf->{runtype} eq "primary") {
                print STDOUT
                  join("\t",$id,$item_form,$date1,$record_type,$bib_lvl,$title)
                    ,"\n";
            } else {
                # case a : isbn and pages
                if (scalar(@isbns)>0 && $pages) {
                    foreach my $isbn ( @isbns ) {
                        print STDOUT
                          join("\t", $id, "case a", $item_form, $date1,
                               $record_type, $bib_lvl, $title, $isbn, $pages)
                            ,"\n";
                    }
                }
                # case b : edition
                if ($edition) {
                    print STDOUT
                      join("\t", $id, "case b", $item_form, $date1,
                           $record_type, $bib_lvl, $title,$edition), "\n";
                }
                # case c : issn
                if ($issn) {
                    print STDOUT join("\t", $id, "case c", $item_form, $date1,
                                      $record_type, $bib_lvl, $title, $issn)
                      ,"\n"; 
                }
                # case d : lccn
                if ($lccn) {
                    print STDOUT join("\t", $id, "case d", $item_form, $date1,
                                      $record_type, $bib_lvl, $title, $lccn)
                      ,"\n";
                }
                # case e : author, publisher, pubyear, pages
                if ($author && $publisher && $pubyear && $pages) {
                    print STDOUT join("\t", $id, "case e", $item_form, $date1,
                                      $record_type, $bib_lvl, $title, $author,
                                      $publisher, $pubyear, $pages), "\n";
                }
            }
        } else {
            print STDERR "Record " . $id . " did not make the cut: ";
            print STDERR "Missing item_form. " unless ($item_form);
            print STDERR "Missing valid date1. " unless ($date1 =~ /\d\d\d\d/);
            print STDERR "Missing record_type. " unless ($record_type);
            print STDERR "Missing bib_lvl. " unless ($bib_lvl);
            print STDERR "Missing title. " unless ($title);
            print STDERR "\n";
        }
    }
    print STDERR "Processed $count records\n";
}

=head2 initialyze

Performs boring script initialization. Handles argument parsing,
mostly.

=cut

sub initialyze {
    my ($c) = @_;
    my @missing = ();

    # set mode on existing filehandles
    binmode(STDOUT, ':utf8');
    binmode(STDIN, ':utf8');

    my $rc = GetOptions( $c,
                         'runtype|r=s',
                         'tag|t=s',
                         'subfield|s=s',
                       );
    show_help() unless $rc;
    my @keys = keys %{$c};
    show_help() unless (@ARGV and @keys);

    for my $key ('runtype', 'tag', 'subfield') {
        push @missing, $key unless $c->{$key}
    }
    if (@missing) {
        print "Required option: ", join(', ', @missing), " missing!\n";
        show_help();
    }
}

=head2 show_help

=cut

sub show_help {
print <<HELP;
Usage is: fingerprinter [REQUIRED ARGS] [OPTIONS] <filelist>
Req'd Arguments
  --runtype=(primary|full) -r  Do 'primary' or 'full' fingerprinting
  --tag=N                  -t  Which tag to use
  --subfield=X             -s  Which subfield to use
Options
  None yet...
HELP
exit 1;
}
