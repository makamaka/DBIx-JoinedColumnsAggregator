require 5.00801;
use strict;
use ExtUtils::MakeMaker;

WriteMakefile(
    'NAME'          => 'DBIx::JoinedColumnsAggregator',
    'VERSION_FROM'  => 'lib/DBIx/JoinedColumnsAggregator.pm', # finds $VERSION
    'PREREQ_PM'     => {
              'Test::More'  => 0,
              'DBD::SQLite' => 0,
    },
    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM => 'lib/DBIx/JoinedColumnsAggregator.pm', # retrieve abstract from module
       AUTHOR     => 'Makamaka Hannyaharamitu, E<lt>makamaka[at]cpan.orgE<gt>') : ()),
    ( $ExtUtils::MakeMaker::VERSION >= 6.3002 ? ('LICENSE' => 'perl', ) : () ),
    ( $ExtUtils::MakeMaker::VERSION >= 6.46 ? (
        'META_MERGE' => {
            resources => {
                repository  =>  'http://github.com/makamaka/DBIx-JoinedColumnsAggregator',
            },
        } ) : ()
    ),
);
