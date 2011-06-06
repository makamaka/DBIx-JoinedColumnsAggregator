use lib './t';
use Test::More;
use strict;
use warnings;

BEGIN {
    use_ok( 'DBIx::JoinedColumnsAggregator' );
}



eval { aggregate_joined_columns( [], {} ) };
ok( $@ );

my $list = aggregate_joined_columns( [], {
    pk => ['id'],
    refs => {
        books   => ['book_id', 'title'],
    },
} );

ok( ref($list) eq 'ARRAY' );

is( scalar(@$list), 0 );


done_testing;

