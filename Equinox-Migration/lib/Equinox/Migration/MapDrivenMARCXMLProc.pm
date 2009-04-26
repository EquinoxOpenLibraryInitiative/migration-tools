package Equinox::Migration::MapDrivenMARCXMLProc;

use warnings;
use strict;

use XML::Twig;
use Equinox::Migration::SubfieldMapper 1.002;

=head1 NAME

Equinox::Migration::MapDrivenMARCXMLProc

=head1 VERSION

Version 1.000

=cut

our $VERSION = '1.000';


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

There is an optional third, argument, C<sample>, which specifies a
arrayref of datafields to "sample" by reporting on subfields which are
found in the data but not in the map.

    my $m = Equinox::Migration::MapDrivenMARCXMLProc->new( mapfile  => FILE,
                                                           marcfile => FILE,
                                                           sample   => \@TAGS
                                                         );

See L</UNMAPPED TAGS> for more info.

=cut

sub new {
    my ($class, %args) = @_;

    my $self = bless { mods => { multi    => {},
                                 once     => {},
                                 required => {},
                               },
                       data => { recs => undef, # X::T record objects
                                 rptr => 0,     # next record pointer
                                 crec => undef, # parsed record storage
                                 stag => undef, # list of tags to sample
                                 umap => undef, # unmapped data samples
                               },
                     }, $class;

    # initialize map and taglist
    die "Argument 'mapfile' must be specified\n" unless (defined $args{mapfile});
    my @mods = keys %{$self->{mods}};
    $self->{map} = Equinox::Migration::SubfieldMapper->new( file => $args{mapfile},
                                                            mods => \@mods );
    $self->{data}{tags} = $self->{map}->tags;

    # initialize twig
    die "Argument 'marcfile' must be specified\n" unless (defined $args{marcfile});
    if (-r $args{marcfile}) {
        $self->{twig} = XML::Twig->new;
        $self->{twig}->parsefile($args{marcfile});
        my @records = $self->{twig}->root->children;
        $self->{data}{recs} = \@records;
    } else {
        die "Can't open marc file: $!\n";
    }

    # if we have a sample arg, set up the sample set and umap hash
    if (defined $args{sample}) {
        for my $s ( @{$args{sample}})
          { $self->{data}{stag}{$s} = 1 }
        $self->{data}{umap} = {};
    }

    return $self;
}


=head2 parse_record

Extracts data from the next record, per the mapping file. Returns a
normalized datastructure (see L</format_record> for details) on
success; returns 0 otherwise.

    while (my $rec = $m->parse_record) {
      # handle extracted record data
    }

=cut

sub parse_record {
    my ($self) = @_;

    # get the next record and wipe current parsed record
    return 0 unless defined $self->{data}{recs}[ $self->{data}{rptr} ];
    my $record = $self->{data}{recs}[ $self->{data}{rptr} ];
    $self->{data}{crec} = { egid => undef, bib  => undef, tags => undef };

    my @fields = $record->children;
    for my $f (@fields)
      { $self->process_field($f) }

    # cleanup memory and increment pointer
    $record->purge;
    $self->{data}{rptr}++;

    return $self->{data}{crec};
}

sub process_field {
    my ($self, $field) = @_;
    my $map = $self->{map};
    my $tag = $field->{'att'}->{'tag'};
    my $crec = $self->{data}{crec};

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
    if ($map->has($tag)) {
        push @{$crec->{tags}}, { tag => $tag, uni => undef, multi => undef };
        my @subs = $field->children('subfield');
        for my $sub (@subs)
          { $self->process_subs($tag, $sub) }
        # check map to ensure all declared subs have a value
        my $mods = $map->mods($field);
        for my $mappedsub ( @{ $map->subfields($tag) } ) {
            next if $mods->{multi};
            $crec->{tags}[-1]{uni}{$mappedsub} = ''
              unless defined $crec->{tags}[-1]{uni}{$mappedsub};
        }
    }
}

sub process_subs {
    my ($self, $tag, $sub) = @_;
    my $map  = $self->{map};
    my $code = $sub->{'att'}->{'code'};

    # handle unmapped tag/subs
    unless ($map->has($tag, $code)) {
        my $u = $self->{data}{umap};
        my $s = $self->{data}{stag};
        return unless (defined $s->{$tag});

        # set a value, total-seen count and records-seen-in count
        $u->{$tag}{$code}{value} = $sub->text unless defined $u->{$tag}{$code};
        $u->{$tag}{$code}{count}++;
        $u->{$tag}{$code}{rcnt}++ unless ( defined $u->{$tag}{$code}{last} and
                                           $u->{$tag}{$code}{last} == $self->{data}{rptr} );
        $u->{$tag}{$code}{last} = $self->{data}{rptr};
        return;
    }

    # fetch our datafield struct and fieldname
    my $dataf = $self->{data}{crec}{tags}[-1];
    my $field = $map->field($tag, $code);

    # handle modifiers, or slug data in normally
    if (my $mods = $map->mods($field)) {
        if ($mods->{multi}) {
            my $name = $tag . $code;
            push @{$dataf->{multi}{$name}}, $sub->text;
        }
    } else {
        $dataf->{uni}{$code} = $sub->text;
    }
}

=head1 PARSED RECORDS

Given:

    my $m = Equinox::Migration::MapDrivenMARCXMLProc->new(ARGUMENTS);
    $rec = $m->parse_record;

Then C<$rec> will look like:

    {
      egid   => evergreen_record_id,
      bib    => {
                  (tag_id . sub_code)1 => value1,
                  (tag_id . sub_code)2 => value2,
                  ...
                },
      tags => [
                {
                  tag   => tag_id,
                  multi => { (tag_id . sub_code) => [ val1, val2, ... ] },
                  uni   => { code => value, code2 => value2, ... },
                },
                ...
              ]
    }

That is, there is an C<egid> key which points to the Evergreen ID of
that record, a C<bib> key which points to a hashref, and a C<tags>
key which points to an arrayref.

=head3 C<bib>

A reference to a hash which holds extracted data which occurs only
once per record (and is therefore "bib-level"; the default assumption
is that a tag/subfield pair can occur multiple times per record). The
keys are composed of tag id and subfield code, catenated
(e.g. 901c). The values are the contents of that subfield of that tag.

If there are no tags defined as bib-level in the mapfile, C<bib> will
be C<undef>.

=head3 C<tags>

A reference to a list of anonymous hashes, one for each instance of
each tag which occurs in the map.

Each tag hash holds its own id (e.g. C<998>), and two references to
two more hashrefs, C<multi> and C<uni>.

The C<multi> hash holds the extracted data for tag/sub mappings which
have the C<multiple> modifier on them. The keys in C<multi> are
composed of the tag id and subfield code, catenated
(e.g. C<901c>). The values are arrayrefs containing the content of all
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

=head1 UNMAPPED TAGS

If the C<sample> argument is passed to L</new>, there will also be a
structure which holds data about unmapped subfields encountered in
mapped tags which are also in the declared sample set. This
information is collected over the life of the object and is not reset
for every record processed (as the current record data neccessarily
is).

    { tag_id => {
                  sub_code  => { value => VALUE,
                                 count => COUNT,
                                 rcnt => RCOUNT
                               },
                  ...
                },
      ...
    }

For each mapped tag, for each unmapped subfield, there is a hash of
data about that subfield containing

    * value - A sample of the subfield text
    * count - Total number of times the subfield was seen
    * rcnt  - The number of records the subfield was seen in

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
