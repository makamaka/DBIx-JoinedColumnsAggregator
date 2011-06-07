use lib './t';
use Test::More;
use strict;
use warnings;
use DBIx::JoinedColumnsAggregator;
use Data::Dumper;


my $objects = [
    { id => 1, col1 => "", col2 => "", col3 => "" },
    { id => 1, col1 => "", col2 => undef, col3 => "" },
    { id => 1, col1 => "", col2 => "\0NULL\0", col3 => "" },
];


my $itr = DummyTestIterator->new( $objects );

my $list = aggregate_joined_columns( $itr, {
    pk => ['id'],
    tags => {
        test   => [qw/col1 col2 col3/],
    },
    access_style => 'hash',
} );

is(@$list, 1);
is(@{ $list->[0]->{test} }, 2); # XXX: we want 3 rows


$objects = [
    { id => 1, col1 => undef },
    { id => 2, col1 => "" },
];

$itr = DummyTestIterator->new( $objects );

$list = aggregate_joined_columns( $itr, {
    pk => ['id'],
    tags => {
        test   => [qw/col1/],
    },
    access_style => 'hash',
} );

is(@$list, 2);
is(@{ $list->[0]->{test} }, 0);
is(@{ $list->[1]->{test} }, 1);


$objects = [
    { id => 1, col1 => undef, col2 => undef },
    { id => 2, col1 => "", col2 => undef },
];

$itr = DummyTestIterator->new( $objects );

$list = aggregate_joined_columns( $itr, {
    pk => ['id'],
    tags => {
        test   => [qw/col1 col2/],
    },
    access_style => 'hash',
} );

is(@$list, 2);
is(@{ $list->[0]->{test} }, 0);
is(@{ $list->[1]->{test} }, 1);



done_testing;


package DummyTestIterator;

sub new {
    my ( $class, $list_ref ) = @_;
    bless { data => $list_ref, idx => 0 }, $class;
}


sub next {
    $_[0]->{ data }->[ $_[0]->{ idx }++ ];
}



