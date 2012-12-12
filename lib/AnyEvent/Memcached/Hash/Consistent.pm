package AnyEvent::Memcached::Hash::Consistent;
our @ISA = 'AnyEvent::Memcached::Hash';

=head1 NAME

AnyEvent::Memcached::Hash::Consistent - Hash using a consistent hashing
algorithm.

=head1 DESCRIPTION

This module hashes using L<Set::ConsistentHash> as a hashing algorithm.  The
result is that the distribution of keys is consistently displaced across 1024
"virtual" buckets, and adding a new memcached server has a network-wide
cache-miss ratio of n / e until 1024 systems.

=cut

use common::sense;
use Set::ConsistentHash;

=head1 CONSTRUCTOR

=over 4

=item new LOTS_OF_THINGS.

Accepts a balanced list of attributes.

=over 10

=item buckets

AnyEvent::Memcached::Buckets

=back

=cut

sub new {
    my ($class) = shift;

    my $self = $class->SUPER::new(@_);

    my $hash = $self->{hash} = Set::ConsistentHash->new;

    # Ignore the bucketer....just use it for a set of peers.
    my $peers = $self->{buckets}->peers;

    $hash->set_targets( map +($_ => 1), keys %$peers );
    
    return $self;
}

=back

=head1 METHODS

=over 4

=item servers KEY | [ KEYS... ]

Return a hash of peer keys, each holding a reference to an array of servers to
map to.

=cut

sub servers {
    my ($self, $keys) = @_;

    if (ref $keys ne "ARRAY") {
        $keys = [ $keys ];
    }

    my $hash   = $self->{hash};
    my $result = {};

    for my $key (@$keys) {
        my $peer = $hash->get_target($key);

        push $result->{$peer} //= [], $key;
    }

    return $result;
}

=back

=cut

return __PACKAGE__;
