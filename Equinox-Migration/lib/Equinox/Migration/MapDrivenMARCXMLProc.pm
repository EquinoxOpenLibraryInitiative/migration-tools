package Equinox::Migration::MapDrivenMARCXMLProc;

use warnings;
use strict;

use XML::Twig;
use Equinox::Migration::SubfieldMapper 1.003;

# FIXME
#
# sample functionality should be extracted into a new module which
# uses E::M::SM to drive sampling of individual datafields, and
# reports ALL datafields which occur
#
# --sample should give the list of all datafields
# --samplefile should take a SM map as teh argument and introspect the mapped datafields


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

=cut

sub new {
    my ($class, %args) = @_;

    my $self = bless { mods => { multi    => {},
                                 bib      => {},
                                 required => {},
                               },
                       data => { recs => undef, # X::T record objects
                                 rptr => 0,     # next record pointer
                                 crec => undef, # parsed record storage
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

    # check for required fields
    $self->check_required;

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
    return unless ($map->has($tag, $code));

    # fetch our datafield struct and fieldname
    my $dataf = $self->{data}{crec}{tags}[-1];
    my $field = $map->field($tag, $code);

    # handle modifiers
    if (my $mods = $map->mods($field)) {
        if ($mods->{multi}) {
            my $name = $tag . $code;
            push @{$dataf->{multi}{$name}}, $sub->text;
            return;
        }
    }

    die "Multiple occurances of a non-multi field: $tag$code at rec ",
      ($self->{data}{rptr} + 1),"\n" if (defined $dataf->{uni}{$code});
    $dataf->{uni}{$code} = $sub->text;
}


sub check_required {
    my ($self) = @_;
    my $mods = $self->{map}->mods;
    my $crec = $self->{data}{crec};

    for my $tag_id (keys %{$mods->{required}}) {
        for my $code (@{$mods->{required}{$tag_id}}) {
            my $found = 0;

            $found = 1 if ($crec->{bib}{($tag_id . $code)});
            for my $tag (@{$crec->{tags}}) {
                $found = 1 if ($tag->{multi}{($tag_id . $code)});
                $found = 1 if ($tag->{uni}{$code});
            }

            die "Required mapping $tag_id$code not found in rec ",$self->{data}{rptr},"\n"
              unless ($found);
        }
    }

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

=head2 bib

The C<bib> modifier declares that a mapping is "bib-level", and should
be encountered once per B<record> instead of once per B<datafield> --
which is another way of saying that it occurs in a non-repeating
datafield or in a controlfield.

=head2 required

By default, if a mapping does not occur in a datafield (or record, in
the case of C<bib> mappings), processing continues normally. if a
mapping has the C<required> modifier, however, it must appear, or a
fatal error will occur.

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
