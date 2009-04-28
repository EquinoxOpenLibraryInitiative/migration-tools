package Equinox::Migration::MARCXMLSampler;

use warnings;
use strict;

use XML::Twig;
use Equinox::Migration::SimpleTagList 1.001;

# FIXME
#
# sample functionality should be extracted into a new module which
# uses E::M::SM to drive sampling of individual datafields, and
# reports ALL datafields which occur
#
# --sample should give the list of all datafields
# --samplefile should take a SM map as teh argument and introspect the mapped datafields


=head1 NAME

Equinox::Migration::MARCXMLSampler

=head1 VERSION

Version 1.000

=cut

our $VERSION = '1.000';


=head1 SYNOPSIS

Foo

    use Equinox::Migration::MARCXMLSampler;


=head1 METHODS


=head2 new

=cut

sub new {
    my ($class, %args) = @_;

    my $self = bless { data => { recs => undef, # X::T record objects
                                 rcnt => 0,     # next record counter
                                 samp => {},    # data samples
                                 tags => {},    # all found tags
                               },
                     }, $class;

    # initialize twig
    die "Argument 'marcfile' must be specified\n" unless ($args{marcfile});
    if (-r $args{marcfile}) {
        $self->{twig} = XML::Twig->new;
        $self->{twig}->parsefile($args{marcfile});
        my @records = $self->{twig}->root->children;
        $self->{data}{recs} = \@records;
    } else {
        die "Can't open marc file: $!\n";
    }

    # if we have a sample arg, create the sample map
    $self->{map} = Equinox::Migration::SimpleTagList->new(file => $args{mapfile})
        if ($args{mapfile});
    $self->{map} = Equinox::Migration::SimpleTagList->new(str => $args{mapstring})
        if ($args{mapstring});

    return $self;
}


=head2 parse_records

Extracts data from MARC records, per the mapping file.

=cut

sub parse_records {
    my ($self) = @_;

    for my $record ( @{$self->{data}{recs}} ) {
        my @fields = $record->children;
        for my $f (@fields)
          { $self->process_field($f) }

        # cleanup memory and increment pointer
        $record->purge;
        $self->{data}{rcnt}++;
    }
}

sub process_field {
    my ($self, $field) = @_;
    my $map = $self->{map};
    my $tag = $field->{'att'}->{'tag'};
    return unless ($tag and $tag > 9);

    # increment raw tag count
    $self->{data}{tags}{$tag}++;

    if ($map and $map->has($tag)) {
        my @subs = $field->children('subfield');
        for my $sub (@subs)
          { $self->process_subs($tag, $sub) }
    }
}

sub process_subs {
    my ($self, $tag, $sub) = @_;
    my $map  = $self->{map};
    my $code = $sub->{'att'}->{'code'};

    # handle unmapped tag/subs
    my $samp = $self->{data}{samp};
    # set a value, total-seen count and records-seen-in count
    $samp->{$tag}{$code}{value} = $sub->text unless defined $samp->{$tag}{$code};
    $samp->{$tag}{$code}{count}++;
    $samp->{$tag}{$code}{rcnt}++ unless ( defined $samp->{$tag}{$code}{last} and
                                          $samp->{$tag}{$code}{last} == $self->{data}{rcnt} );
    $samp->{$tag}{$code}{last} = $self->{data}{rcnt};
}


=head1 SAMPLED TAGS

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

    perldoc Equinox::Migration::MARCXMLSampler


=head1 COPYRIGHT & LICENSE

Copyright 2009 Equinox, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of Equinox::Migration::MARCXMLSampler
