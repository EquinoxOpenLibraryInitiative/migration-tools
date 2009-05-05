package Equinox::Migration::MARCXMLSampler;

use warnings;
use strict;

use XML::Twig;
use Equinox::Migration::SimpleTagList 1.001;


=head1 NAME

Equinox::Migration::MARCXMLSampler

=head1 VERSION

Version 1.000

=cut

our $VERSION = '1.000';


=head1 SYNOPSIS

Produce a list of all fields in a MARCXML file which have a C<tag>
attribute, and count how many times each occurs

    my $s =  E::M::MARCXMLSampler->new( marcfile => "foo.marc.xml" );
    $s->parse_records;

Also deeply introspect certain tags, producing lists of all subfields,
and counts of how many times each subfield occurs I<in toto> and how
many records each subfield appears in

    my $s = E::M::MARCXMLSampler->new( marcfile => "foo.marc.xml",
                                       mapfile  => "foo.map" );
             ~ or ~
    
    my $s = E::M::MARCXMLSampler->new( marcfile  => "foo.marc.xml",
                                       mapstring => "852 999" );
    $s->parse_records;


=head1 METHODS


=head2 new

Takes one required argument, C<marcfile>, which points to the MARCXML
file to be processed.

Has two mutually-exclusive optional arguments, C<mapfile> and
C<mapstring>". The former should point to a file which will be used as
a L<Equinox::Migration::SimpleTagList> map; the latter should have as
its value a text string which will be used in the same way (handy for
when you only want deep introspection on a handful of tags).

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
    die "Can't use a mapfile and mapstring\n"
      if ($args{mapfile} and $args{mapstring});
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
    #FIXME tcnt not rcnt
}


=head1 SAMPLED TAGS

If the C<mapfile> or C<mapstring> arguments are passed to L</new>, a
structure will be constructed which holds data about tags in the map.

    { tag_id => {
                  sub_code  => { value => VALUE,
                                 count => COUNT,
                                 rcnt => RCOUNT
                               },
                  ...
                },
      ...
    }

For each subfield in each mapped tag, there is a hash of data about
that subfield containing

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
