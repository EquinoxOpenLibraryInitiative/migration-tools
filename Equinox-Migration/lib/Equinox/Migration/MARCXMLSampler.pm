package Equinox::Migration::MARCXMLSampler;

use warnings;
use strict;

use XML::Twig;
use Equinox::Migration::SimpleTagList 1.001;


=head1 NAME

Equinox::Migration::MARCXMLSampler

=head1 VERSION

Version 1.003

=cut

our $VERSION = '1.003';

my $xmltwig;
my $taglist;
my $dstore;


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

    $dstore = { rcnt => 0,     # record counter
                tcnt => 0,     # tag counter
                scnt => {},    # subfield/tag counters
                samp => {},    # data samples
                tags => {},    # all found tags
              };

    my $self = bless { data => $dstore,
                     }, $class;

    # initialize twig
    die "Argument 'marcfile' must be specified\n" unless ($args{marcfile});
    if (-r $args{marcfile}) {
        $xmltwig = XML::Twig->new( twig_handlers => { record => \&parse_record } );
        $self->{conf}{marc} = $args{marcfile};
    } else {
        die "Can't open marc file: $!\n";
    }

    # if we have a sample arg, create the sample map
    die "Can't use a mapfile and mapstring\n"
      if ($args{mapfile} and $args{mapstring});
    $taglist = Equinox::Migration::SimpleTagList->new(file => $args{mapfile})
        if ($args{mapfile});
    $taglist = Equinox::Migration::SimpleTagList->new(str => $args{mapstring})
        if ($args{mapstring});

    # do the xml processing
    $xmltwig->parsefile( $self->{conf}{marc} );

    # hand ourselves back for 
    return $self;
}


=head2 parse_record

XML::Twig handler for record elements; drives data extraction process.

=cut

sub parse_record {
    my ($twig, $record) = @_;

    my @fields = $record->children;
    for my $f (@fields)
      { process_field($f) }

    # cleanup memory and increment pointer
    $record->purge;
    $dstore->{rcnt}++;
}


sub process_field {
    my ($field) = @_;
    my $tag = $field->{'att'}->{'tag'};
    return unless ($tag and $tag > 9);

    # increment raw tag count
    $dstore->{tcnt}++;
    $dstore->{tags}{$tag}++;


    if ($taglist and $taglist->has($tag)) {
        my @subs = $field->children('subfield');
        my $i= 0;
        for my $sub (@subs)
          { process_subs($tag, $sub); $i++ }

        # increment sub length counter
        $dstore->{scnt}{$tag}{$i}++;
    }
}

sub process_subs {
    my ($tag, $sub) = @_;
    my $code = $sub->{'att'}->{'code'};

    # handle unmapped tag/subs
    my $samp = $dstore->{samp};
    # set a value, total-seen count and records-seen-in count
    $samp->{$tag}{$code}{value} = $sub->text unless $samp->{$tag}{$code};
    $samp->{$tag}{$code}{count}++;
    $samp->{$tag}{$code}{tcnt}++ unless ( defined $samp->{$tag}{$code}{last} and
                                          $samp->{$tag}{$code}{last} == $dstore->{tcnt} );
    $samp->{$tag}{$code}{last} = $dstore->{tcnt};
}


=head1 SAMPLED TAGS

If the C<mapfile> or C<mapstring> arguments are passed to L</new>, a
structure will be constructed which holds data about tags in the map.

    { tag_id => {
                  sub_code  => { value => VALUE,
                                 count => COUNT,
                                 tcnt  => TAGCOUNT
                               },
                  ...
                },
      ...
    }

For each subfield in each mapped tag, there is a hash of data about
that subfield containing

    * value - A sample of the subfield text
    * count - Total number of times the subfield was seen
    * tcnt  - The number of tags the subfield was seen in

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
