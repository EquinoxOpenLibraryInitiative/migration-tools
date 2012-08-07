package Equinox::Migration::MapDrivenMARCXMLProc;

# Copyright 2009-2012, Equinox Software, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

use warnings;
use strict;

use XML::Twig;
use Equinox::Migration::SubfieldMapper 1.004;


=head1 NAME

Equinox::Migration::MapDrivenMARCXMLProc

=head1 VERSION

Version 1.005

=cut

our $VERSION = '1.005';

my $dstore;
my $sfmap;
my @modlist = qw( multi ignoremulti bib required first concatenate parallel );
my %allmods = ();
my $multis = {};
my $parallel_fields = {};
my $reccount;
my $verbose = 0;


=head1 SYNOPSIS

Foo

    use Equinox::Migration::MapDrivenMARCXMLProc;


=head1 METHODS


=head2 new

Takes two required arguments: C<mapfile> (which will be passed along
to L<Equinox::Migration::SubfieldMapper> as the basis for its map),
and C<marcfile> (the MARC data to be processed).

    my $m = Equinox::Migration::MapDrivenMARCXMLProc->new( mapfile  => FILE,
                                                           marcfile => FILE );

=cut

sub new {
    my ($class, %args) = @_;

    $verbose = 1 if $args{verbose};

    my $self = bless {
                        multis => \$multis,
                        parallel_fields => \$parallel_fields,
                     }, $class;

    # initialize map and taglist
    die "Argument 'mapfile' must be specified\n" unless ($args{mapfile});
    $sfmap = Equinox::Migration::SubfieldMapper->new( file => $args{mapfile},
                                                      mods => \@modlist );

    # initialize datastore
    $dstore = {};
    $reccount = 0;            # next record ptr
    $dstore->{tags} = $sfmap->tags; # list of all tags
    $self->{data} = $dstore;

    # initialize twig
    die "Argument 'marcfile' must be specified\n" unless ($args{marcfile});
    if (-r $args{marcfile}) {
        my $xmltwig = XML::Twig->new( twig_handlers => { record => \&parse_record } );
        $xmltwig->parsefile( $args{marcfile} );
    } else {
        die "Can't open marc file: $!\n";
    }

    return $self;
}

=head2 parse_record

Extracts data from the next record, per the mapping file.

=cut

sub parse_record {
    my ($twig, $record) = @_;
    my $crec = {}; # current record

    my @fields = $record->children;
    for my $f (@fields)
      { process_field($f, $crec) }

    # fill in blank values if needed
    for my $mappedtag ( @{ $sfmap->tags }) {
        unless (exists $crec->{tmap}{$mappedtag}) {
            push @{ $crec->{tags} }, {};
            for my $mappedsub ( @{ $sfmap->subfields($mappedtag) } ) {
                my $fieldname = $sfmap->field($mappedtag, $mappedsub);
                my $mods = $sfmap->mods($fieldname);
                next if $mods->{multi};
                if ($mods->{parallel}) {
                    push @{ $crec->{tags}[-1]{parallel}{$mappedsub} }, '';
                    $crec->{tags}[-1]{uni} = undef;
                } else {
                    $crec->{tags}[-1]{uni}{$mappedsub} = '';
                    $crec->{tags}[-1]{parallel} = undef;
                }
                $crec->{tags}[-1]{multi} = undef;
                $crec->{tags}[-1]{tag} = $mappedtag;
            }
            push @{ $crec->{tmap}{$mappedtag} }, $#{ $crec->{tags} };
        }
    }

    # cleanup memory and increment pointer
    $record->purge;
    $reccount++;

    # check for required fields
    check_required($crec);
    push @{ $dstore->{recs} }, $crec;

    print STDERR "$reccount\n"
      if ($verbose and !($reccount % 1000));
}

sub process_field {
    my ($field, $crec) = @_;
    my $tag = $field->{'att'}->{'tag'};

    # leader
    unless (defined $tag) {
        #FIXME
        return;
    }

    # datafields
    if ($tag == 903) {
        my $sub = $field->first_child('subfield');
        $crec->{egid} = $sub->text;
        return;
    }
    if ($sfmap->has($tag)) {
        push @{$crec->{tags}}, { tag => $tag, uni => undef, multi => undef, parallel => undef };
        push @{$crec->{tmap}{$tag}}, (@{$crec->{tags}} - 1);
        my @subs = $field->children('subfield');
        for my $sub (@subs)
          { process_subs($tag, $sub, $crec) }

        # check map to ensure all declared tags and subs have a value
        for my $mappedsub ( @{ $sfmap->subfields($tag) } ) {
            my $fieldname = $sfmap->field($tag, $mappedsub);
            my $mods = $sfmap->mods($fieldname);
            next if $mods->{multi};
            if ($mods->{parallel}) {
                push @{ $crec->{tags}[-1]{parallel}{$mappedsub} }, ''
                    unless defined $crec->{tags}[-1]{parallel}{$mappedsub};
            } else {
                $crec->{tags}[-1]{uni}{$mappedsub} = ''
                    unless defined $crec->{tags}[-1]{uni}{$mappedsub};
            }
        }
    }
}

sub process_subs {
    my ($tag, $sub, $crec) = @_;
    my $code = $sub->{'att'}->{'code'};

    # handle unmapped tag/subs
    return unless ($sfmap->has($tag, $code));

    # fetch our datafield struct and field and mods
    my $dataf = $crec->{tags}[-1];
    my $field = $sfmap->field($tag, $code);
    my $sep = $sfmap->sep($field);
    $allmods{$field} = $sfmap->mods($field) unless $allmods{$field};
    my $mods = $allmods{$field};

    # test filters
    for my $filter ( @{$sfmap->filters($field)} ) {
        return if ($sub->text =~ /$filter/i);
    }

    if ($mods->{parallel}) {
        $parallel_fields->{$tag}{$code} = 1;
        push @{$dataf->{parallel}{$code}}, $sub->text;
        return;
    }

    # handle multi modifier
    if ($mods->{multi}) {
        $multis->{$tag}{$code} = 1;
        if ($mods->{concatenate}) {
            if (exists($dataf->{multi}{$code})) {
                $dataf->{multi}{$code}[0] .= $sep . $sub->text;
            } else {
                push @{$dataf->{multi}{$code}}, $sub->text;
            }
            $multis->{$tag}{$code} = 1;
        } else {
            push @{$dataf->{multi}{$code}}, $sub->text;
        }
        return;
    }


    if ($mods->{concatenate}) {
        if (exists($dataf->{uni}{$code})) {
            $dataf->{uni}{$code} .= $sep . $sub->text;
        } else {
            $dataf->{uni}{$code} = $sub->text;
        }
        return;
    }

    # if this were a multi field, it would be handled already. make sure its a singleton
    die "Multiple occurances of a non-multi field: $tag$code at rec ",
      ($reccount + 1),"\n" if (defined $dataf->{uni}{$code} and !$mods->{ignoremulti});

    # everything seems okay
    $dataf->{uni}{$code} = $sub->text;
}


sub check_required {
    my ($crec) = @_;
    my $mods = $sfmap->mods;

    for my $tag_id (keys %{$mods->{required}}) {
        for my $code (@{$mods->{required}{$tag_id}}) {
            my $found = 0;

            for my $tag (@{$crec->{tags}}) {
                $found = 1 if ($tag->{multi}{($tag_id . $code)});
                $found = 1 if ($tag->{uni}{$code});
            }

            die "Required mapping $tag_id$code not found in rec ",$reccount,"\n"
              unless ($found);
        }
    }

}

=head2 recno

Returns current record number (starting from zero)

=cut

sub recno { my ($self) = @_; return $self->{data}{rcnt} }

=head2 name

Returns mapped fieldname when passed a tag, and code

    my $name = $m->name(999,'a');

=cut

sub name { my ($self, $t, $c) = @_; return $sfmap->field($t, $c) }

=head2 first_only

Returns whether mapped fieldname is to be applied only to first
item in a bib

=cut

sub first_only {
    my ($self, $t, $c) = @_;
    my $field = $sfmap->field($t, $c);
    my $mods = $sfmap->mods($field);
    return exists($mods->{first});
}

=head2 get_multis

Returns hashref of C<{tag}{code}> for all mapped multi fields

=cut

sub get_multis {
    my ($self) = @_;
    return $multis;
}

=head2 get_parallel_fields

Returns hashref of C<{tag}{code}> for all mapped parallel fields

=cut

sub get_parallel_fields {
    my ($self) = @_;
    return $parallel_fields;
}

=head1 MODIFIERS

MapDrivenMARCXMLProc implements the following modifiers, and passes
them to L<Equinox::Migration::SubfieldMapper>, meaning that specifying
any other modifiers in a MDMP map file will cause a fatal error when
it is processed.

=head2 multi

If a mapping is declared to be C<multi>, then MDMP expects to see more
than one instance of that subfield per datafield, and the data is
handled accordingly (see L</PARSED RECORDS> below).

Occurring zero or one time is legal for a C<multi> mapping.

A mapping which is not flagged as C<multi>, but which occurs more than
once per datafield will cause a fatal error.

=head2 required

By default, if a mapping does not occur in a datafield, processing
continues normally. if a mapping has the C<required> modifier,
however, it must appear, or a fatal error will occur.

=head1 PARSED RECORDS

Given:

    my $m = Equinox::Migration::MapDrivenMARCXMLProc->new(ARGUMENTS);
    $rec = $m->parse_record;

Then C<$rec> will look like:

    {
      egid => evergreen_record_id,
      tags => [
                {
                  tag   => tag_id,
                  multi => { code => [ val1, val2, ... ] },
                  uni   => { code => value, code2 => value2, ... },
                },
                ...
              ],
      tmap => { tag_id => [ INDEX_LIST ], tag_id2 => [ INDEX_LIST ], ... }
    }

That is, there is an C<egid> key which points to the Evergreen ID of
that record, a C<tags> key which points to an arrayref, and a C<tmap>
key which points to a hashref.

=head3 C<tags>

A reference to a list of anonymous hashes, one for each instance of
each tag which occurs in the map.

Each tag hash holds its own id (e.g. C<998>), and two references to
two more hashrefs, C<multi> and C<uni>.

The C<multi> hash holds the extracted data for tag/sub mappings which
have the C<multiple> modifier on them. The keys in C<multi> subfield
codes.  The values are arrayrefs containing the content of all
instances of that subfield in that instance of that tag. If no tags
are defined as C<multi>, it will be C<undef>.

The C<uni> hash holds data for tag/sub mappings which occur only once
per instance of a tag (but may occur multiple times in a record due to
there being multiple instances of that tag in a record). Keys are
subfield codes and values are subfield content.

All C<uni> subfields occuring in the map are guaranteed to be
defined. Sufields which are mapped but do not occur in a particular
datafield will be given a value of '' (the null string) in the current
record struct. Oppose subfields which are not mapped, which will be
C<undef>.

=head3 tmap

A hashref, where each key (a tag id like "650") points to a listref
containing the index (or indices) of C<tags> where that tag has
extracted data.

The intended use of this is to simplify the processing of data from
tags which can appear more than once in a MARC record, like
holdings. If your holdings data is in 852, C<tmap->{852}> will be a
listref with the indices of C<tags> which hold the data from the 852
datafields.

Complimentarily, C<tmap> prevents data from singular datafields from
having to be copied for every instance of a multiple datafield, as it
lets you get the data from that record's one instance of whichever
field you're looking for.

=head1 AUTHOR

Shawn Boyette, C<< <sboyette at esilibrary.com> >>

=head1 BUGS

Please report any bugs or feature requests to the above email address.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Equinox::Migration::MapDrivenMARCXMLProc


=head1 COPYRIGHT & LICENSE

Copyright 2009 Equinox, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of Equinox::Migration::MapDrivenMARCXMLProc
