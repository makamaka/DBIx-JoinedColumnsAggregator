use lib './t';
use Test::More;
use strict;
use warnings;
use DBIx::JoinedColumnsAggregator;
use Data::Dumper;

my ($objects, $itr, $list);

$objects = [
    { id => 1, user_id => 1, user_name => 'A', item_id => 1, item_name => 'AAA', item_attr => 'fireproof' },
    { id => 2, user_id => 1, user_name => 'A', item_id => 1, item_name => 'AAA', item_attr => 'waterproof' },
    { id => 3, user_id => 1, user_name => 'A', item_id => 2, item_name => 'BBB', item_attr => undef },
    { id => 4, user_id => 1, user_name => 'A', item_id => 3, item_name => 'CCC', item_attr => 'blessed' },
    { id => 5, user_id => 2, user_name => 'B', item_id => 1, item_name => 'AAA', item_attr => 'blessed' },
];


my @tests = (
    { items => [ ['item_name' => 'name'], 'item_id', { 'pk' => ['item_id'], tags => { 'attrs' => ['item_attr'] } } ] },
    { items => [ ['item_name' => 'name'], ['item_id' => 'id'], { 'pk' => ['id'], tags => { 'attrs' => ['item_attr'] }, always_hash => 0 } ] },
    { items => [ ['item_name' => 'name'], ['item_id' => 'id'], { 'pk' => ['id'], tags => { 'attrs' => ['item_attr'] } } ] },
    { items => [ ['item_name' => 'name'], ['item_id' => 'id'], { 'pk' => ['id'], tags => { 'attrs' => [['item_attr'=>'hoge']] }, always_hash => 1 } ] },
);

for my $test ( @tests ) {
    my $item_id = ref( $test->{items}->[1] ) ? 'id' : 'item_id';

    $list = aggregate_joined_columns( $objects, {
        pk => ['user_id'],
        tags => $test,
        access_style => 'hash',
        always_hash => 1,
    } );

    is(@$list, 2);
    is( $list->[0]->{user_name}, 'A');
    is( $list->[1]->{user_name}, 'B');

    is(@{ $list->[0]->{items} }, 3);
    is(@{ $list->[1]->{items} }, 1);

    is( $list->[0]->{items}->[1]->{$item_id}, 2);
    is( $list->[0]->{items}->[1]->{name}, 'BBB');
    is(@{ $list->[0]->{items}->[0]->{attrs} }, 2);
    is(@{ $list->[0]->{items}->[1]->{attrs} }, 0);
    is(@{ $list->[0]->{items}->[2]->{attrs} }, 1);

    is(@{ $list->[1]->{items}->[0]->{attrs} }, 1);
}

done_testing;

__END__
