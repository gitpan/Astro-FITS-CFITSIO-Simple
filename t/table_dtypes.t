use Test::More tests => 10;

use strict;
use warnings;

use PDL;

use Astro::FITS::CFITSIO::Simple qw/ :all /;

BEGIN { require 't/common.pl'; }

my $file = 'data/f001.fits';

# success
eval {
  foreach my $type ( float, double, short, long, ushort, byte )
  {
    my %data = rdfits( $file, { dtypes => { rt_x => $type } } );

    ok ( $type == $data{rt_x}->type, "dtype: $type" );
  }
};
ok ( ! $@, "dtype" ) or diag( $@ );


# failure
foreach ( 5, 'snack' )
{
  eval {
    rdfits( $file, { dtypes => { rt_x => $_ } } );
  };
  like ( $@, qr/user specified type/, "dtype: bad type $_" );
}

for ( qw/ rt_snackfood / )
{
  eval {
    rdfits( $file, { dtypes => { $_ => float } } );
  };
  like ( $@, qr/not in file/, "dtype: bad column name $_" );
}
 
