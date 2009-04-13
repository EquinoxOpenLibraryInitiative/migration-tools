package Equinox::Migration::MapDrivenXMLProc;

use warnings;
use strict;

use XML::Twig;
use Equinox::Migration::SubfieldMapper;

=head1 NAME

Equinox::Migration::MapDrivenXMLProc

=head1 VERSION

Version 1.000

=cut

our $VERSION = '1.000';


=head1 SYNOPSIS

Foo

    use Equinox::Migration::MapDrivenXMLProc;


=cut



=head1 METHODS


=head2 new

=cut

sub new {
    my ($class, %args) = @_;

    my $self = bless { conf => { count => 0,
                                 total => 0,
                                 quiet => 0,
                               },
                       map  => Equinox::Migration::SubfieldMapper->new(file => $args{mapfile}),
                       tags => {},
                       twig => XML::Twig->new( twig_handlers => { record => \&record } ),
                     }, $class;

    if ($args{marcfile}) {
        if (-r $args{marcfile}) {
            $self->{conf}{marc} = $args{marcfile};
            $self->generate;
        } else {
            die "Can't open marc file: $!\n";
        }
    }
    $self->{twig}->parsefile($self->{conf}{marc});


    return $self;
}

sub parse {
    my ($self) = @_;
}


sub emit_status {
    my ($self) = @_;
    my $c = $self->{conf};
    return if $c->{quiet};
    $c->{count}++;
    my $percent = int(($c->{count} / $c->{total}) * 100);
    print STDERR "\r$percent% done (", $c->{count}, ")";
}


=head2 XML::Twig CALLBACK ROUTINES

=head3 record

=cut

sub record {
    my($t, $r)= @_;
    $self->{holdings} = {};

    my @dfields = $r->children('datafield');
    for my $d (@dfields) {
        process_datafields($d);
    }
    write_data_out();
    $r->purge;
}

=head3 process_datafields

=cut

sub process_datafields {
    my ($d) = @_;
    my $map = $self->{map};
    my $tag = $d->{'att'}->{'tag'};

    if ($tag == 903) {
        my $s = $d->first_child('subfield');
        $self->{holdings}{id} = $s->text;;
    } elsif ($map->has($tag)) {
        push @{$self->{holdings}{copies}}, { tag => $tag };
        my @subs = $d->children('subfield');
        for my $sub (@subs)
          { process_subs($tag, $sub) }
    }
}

=head3 process_subs

=cut

sub process_subs {
    my ($tag, $sub) = @_;
    my $map  = $self->{map};
    my $code = $sub->{'att'}->{'code'};

    unless ($map->has($tag, $code)) {
        # this is a subfield code we don't have mapped. report on it if this is a sample tag
        push @{$c->{sample}{$tag}}, $code if defined $c->{sample}{tag};
        return;
    }

    my $copy = $self->{holdings}{copies}[-1];
    my $field = $map->field($tag, $code);
            if ($map->mod($field) eq 'multi') {
        my $name = $tag . $code;
        push @{$copy->{multi}{$name}}, $sub->text;
    } else {
        $copy->{uni}{$code} = $sub->text;
    }
}

=head1 AUTHOR

Shawn Boyette, C<< <sboyette at esilibrary.com> >>

=head1 BUGS

Please report any bugs or feature requests to the above email address.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Equinox::Migration::MapDrivenXMLProc


=head1 COPYRIGHT & LICENSE

Copyright 2009 Equinox, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of Equinox::Migration::MapDrivenXMLProc
