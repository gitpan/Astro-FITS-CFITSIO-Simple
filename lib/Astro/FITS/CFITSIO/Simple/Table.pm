package Astro::FITS::CFITSIO::Simple::Table;

use 5.008002;
use strict;
use warnings;

require Exporter;

use Params::Validate qw/ :all /;

use Carp;

use PDL;

use Astro::FITS::CFITSIO qw/ :constants /;
use Astro::FITS::CFITSIO::CheckStatus;
use Astro::FITS::CFITSIO::Simple::PDL qw/ :all /;
use Astro::FITS::Header;
use Astro::FITS::Header::CFITSIO;

our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use Astro::FITS::CFITSIO::Table ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw(
  _rdfitsTable
) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(
	
);

our $VERSION = '0.01';


# this must be called ONLY from rdfits.  it makes assumptions about
# the validity of arguments that have been verified by rdfits.

sub _rdfitsTable
{

  my $opts = 'HASH' eq ref $_[-1] ? pop : {};

  # first arg is fitsfilePtr
  # second is cleanup object; must keep around until we're done,
  # so it'll cleanup at the correct time.
  my $fptr = shift;
  my $cleanup  = shift;

    croak( "column names must be scalars\n" ) if grep { ref $_ } @_;
  my @req_cols = map { lc $_ } @_;

  my %opt = 
    validate_with( params => [ $opts ],
		   normalize_keys => sub{ lc $_[0] },
		   spec =>
		 {
		  rfilter  => { type => SCALAR,  optional => 1 },
		  dtypes   => { type => HASHREF, optional => 1 },
		  ninc     => { type => SCALAR,  optional => 1 },
		  rethash  => { type => SCALAR,  default  => 0 },
		  retinfo  => { type => SCALAR,  default  => 0 },
		  rethdr   => { type => SCALAR,  default  => 0 },
		 } );

  $opt{rethash} = 1 unless @_ || $opt{retinfo};

  # data structure describing the columns
  my %cols;

  # final list of columns (not column names!)
  my @cols;

  tie my $status, 'Astro::FITS::CFITSIO::CheckStatus';

  # normalize column names for user specified types.
  my %user_types = map { lc($_) => $opt{dtypes}{$_} } keys %{$opt{dtypes}};

  # hash of requested column names (if any); used to track
  # non-existant columns
  my %req_cols = map { ( $_ => 0 ) } @req_cols;


  # grab header

  my $hdr = new Astro::FITS::Header::CFITSIO( fitsID => $fptr );

  # grab the number of columns and rows in the HDU
  $fptr->get_num_cols( my $ncols, $status );
  $fptr->get_num_rows( my $nrows, $status);

  my $ninc = $opt{ninc};
  $ninc or $fptr->get_rowsize($ninc, $status);
  $ninc = $nrows if $nrows < $ninc;

  # transfer buffers until we can figure out how to read chunks directly
  # into the final piddles. These are indexed off of the shape of the
  # piddle so that we can reuse them and save memory.
  my %tmppdl;

  # create data structure describing the columns
  for my $coln (1..$ncols) {

    $fptr->get_colname( CASEINSEN, $coln, my $oname, undef, $status);
    my $name = lc $oname;

    # blank name!  can't have that.  unlike PDL::IO::FITS we just
    # number 'em after the actual column position.
    $name = "col_$coln" if  '' eq $name;

    # check for dups; can't have that either! Follow PDL::IO::FITS
    # convention
    if ( exists $cols{$name} )
    {
      my $idx = 1;

      $idx++ while exists $cols{ "${name}_${idx}" };
      $name = "${name}_${idx}";
    }

    # fix up header to track name change
    if ( $name ne lc $oname )
    {
      if ( defined ( my $item = $hdr->itembyname( "TTYPE$coln" ) ) )
      {
	$item->value( uc $name );
      }
      else
      {
	$hdr->insert( -1, new Astro::FITS::Header::Item
		      ( Keyword => "TTYPE$coln",
			Value   => uc $name,
			Comment => 'Label for field',
			Type    => 'string' ) );
      }
    }

    # preset fields used as arguments to CFITSIO as that doesn't seem
    # to  auto-vivify them
    my $col = $cols{$name} = 
    {
     map { $_ => undef } qw/ btype repeat width naxes btype / };

    $col->{n} = $coln;
    $col->{name} = $name;

    # we don't care about a column if it wasn't requested (if any
    # were requested)
    next if @req_cols && ! exists $req_cols{ $name };
    $req_cols{$name}++;

    $fptr->get_eqcoltype( $coln, $col->{btype}, $col->{repeat},
			  $col->{width}, $status );

    # momentarily read into a Perl array, rather than a piddle
    $fptr->perlyunpacking(1);
    $fptr->read_tdim( $coln, my $naxis, $col->{naxes} = [], $status );
    $fptr->perlyunpacking(0);

    # simplify data layout if this is truly a 1D data set (else
    # PDL will create a ( 1 x N ) piddle, which is unexpected.
    # can't get rid of singleton dimensions if this is a n > 1 dim data set
    $col->{naxes} = []
      if $naxis == 1 && 1 == $col->{naxes}[0];

    # figure out what sort of piddle to store the data in
    $col->{ptype} = undef;

    # user specified piddle type?
    if ( exists $user_types{$name} )
    {
      my $type = delete $user_types{$name};

      if ( ! UNIVERSAL::isa( $type, 'PDL::Type' ) )
      {
	croak( "unrecognized user specified type for column '$name'" );
      } elsif (   $col->{btype} == TLOGICAL
		  || $col->{btype} == TBIT
		  || $col->{btype} == TSTRING )
      {
	carp("ignoring user specified type for column '$name': either LOGICAL, BIT or STRING" );
      } else
      {
	$col->{ptype} = $type;
      }

    }

    # user didn't set it?
    eval { 
      $col->{ptype} = fits2pdl_coltype( $col->{btype} )
	unless defined $col->{ptype} 
      };
    croak( "column $col->{name}: $@\n" )
      if $@;

    # what kind of null value do we need?
    $col->{nullval} = $PDL::Bad::Status ? badvalue( $col->{ptype} ) : undef;
    $col->{anynul} = 0;

    # shape of temporary storage for this piddle.
    $col->{tmpshape} = join( ",", $col->{ptype},
			     @{$col->{naxes}}, $ninc );

    # create the storage area
    if ( TSTRING != $col->{btype} ) {

      # create final and temp piddles.
      $col->{data}   = PDL->new_from_specification( $col->{ptype},
						    @{$col->{naxes}}, $nrows );

      # reuse tmppdls
      $tmppdl{$col->{tmpshape}} = 
	PDL->new_from_specification( $col->{ptype},
				     @{$col->{naxes}}, $ninc )
	  unless defined $tmppdl{$col->{tmpshape}};

      $col->{tmppdl} = $tmppdl{$col->{tmpshape}};

      # set up formats for destination and source slices to copy
      # from temp to final destination 
      $col->{dst_slice} = ':,' x @{$col->{naxes}} . '%d:%d';
      $col->{src_slice} = ':,' x @{$col->{naxes}} . '0:%d';

    } else {
      $col->{data} = [];
    }

    # what shall we tell CFITSIO that we're reading? some deception,
    # as all we care about is the size of the data type
    do {
      $col->{btype} == TLOGICAL() and $col->{ctype} = TLOGICAL(), last;
      $col->{btype} == TBYTE()    and $col->{ctype} = TBIT()    , last;
      $col->{btype} == TSTRING()  and $col->{ctype} = undef   , last;
      $col->{ctype} = pdl2cfitsio($col->{ptype});
    };

    # grab extra column information if requested
    if ( $opt{retinfo} )
    {
      $col->{retinfo}{hdr} = {};

      for my $item ( $hdr->itembyname( qr/T\D+$col->{n}$/i ) )
      {
	$item->keyword =~ /(.*?)\d+$/;
	$col->{retinfo}{hdr}{lc $1} = $item->value;
      }

      $col->{retinfo}{idx}  = $col->{n};
    }

  }

  # now, complain about extra parameters
  {
    my @notfound = grep { ! $req_cols{$_} } keys %req_cols;
    croak( "requested column(s) not in file: ", join(", ", @notfound), "\n" )
      if @notfound;

    croak( "user specified type(s) for columns not in file: ", 
	 join(", ", keys %user_types ), "\n" )
      if keys %user_types;
  }

  # construct final list of columns to be read in, either from the
  # list the user provided, or from those in the file (sorted by
  # column number).
  @cols = @req_cols ?
    @cols{@req_cols} :
      sort { $a->{n} <=> $b->{n} } values %cols;


  # scalar context, more than one column returned? doesn't make sense,
  # does it?
  # test for this early, as it may be an expensive mistake...
  croak( "rdfitsTable called in scalar context, but it read more than one column?\n" ) 
    if ! wantarray() && @req_cols > 1;

  # create masks if we'll be row filtering
  my ($good_mask, $tmp_good_mask, $ngood);
  if ($opt{rfilter}) {
    $good_mask = ones(byte,$nrows);
    $tmp_good_mask = ones(byte,$ninc);
    $ngood = 0;
  }

  my $rows_done = 0;
  while ($rows_done < $nrows) {

    my $rows_this_time = $nrows - $rows_done;
    $rows_this_time = $ninc if $rows_this_time > $ninc;

    # row filter
    if ($opt{rfilter}) {
      my $tmp_ngood = 0;
      $fptr->find_rows( $opt{rfilter}, $rows_done+1, $rows_this_time, 
			$tmp_ngood,
			${$tmp_good_mask->get_dataref}, 
			$status = "error filtering rows: rfilter = '$opt{rfilter}'" );
      $tmp_good_mask->upd_data;

      (my $t = $good_mask->mslice( [$rows_done, $rows_done+$rows_this_time-1]) ) .=
	$tmp_good_mask->mslice( [0, $rows_this_time-1] );

      $ngood += $tmp_ngood;

      $tmp_ngood > 0 or
	$rows_done += $rows_this_time,
	  next;
    }

    for my $col ( @cols ) {
      next unless $col->{repeat};

      if (TSTRING != $col->{btype} ) {
	$fptr->read_col( $col->{ctype},
			 $col->{n},
			 $rows_done+1, 1,
			 $col->{repeat} * $rows_this_time,
			 $col->{nullval},
			 ${$col->{tmppdl}->get_dataref},
			 $col->{anynul},
			 $status = "error reading FITS data"
		       );

	$col->{tmppdl}->upd_data;

	my $dest = sprintf($col->{dst_slice}, $rows_done, 
			   $rows_done + $rows_this_time - 1);
	my $src  = sprintf($col->{src_slice}, $rows_this_time - 1);

	(my $t = $col->{data}->slice($dest)) .= $col->{tmppdl}->slice($src);

	$col->{data}->badflag($col->{anynul}) if $PDL::Bad::Status;

      } else {			# string type
	my $tmp = [];
	$fptr->read_col(TSTRING,
			$col->{n},
			$rows_done+1, 1,
			$rows_this_time,
			0,
			$tmp,
			undef,
			$status = "error reading FITS data",
		       );
	push @{$col->{data}}, @$tmp;
      }

    }
    $rows_done += $rows_this_time;
  }

  if ($nrows && $opt{rfilter}) {

    my $good_index = which($good_mask);

    for my $col ( @cols ) {
      next unless $col->{repeat};

      if ( TSTRING != $col->{btype} ) {
	$col->{data} = $col->{data}->dice( ('X') x @{$col->{naxes}},
					   which( $good_mask ) );
      } else {			# string type
	@{$col->{data}} = @{$col->{data}}[$good_index->list];
      }
    }
  }


  # how shall i return the data? let me count the ways...
  if ( $opt{retinfo} )
  {
    # gotta put the data into the retinfo structure.
    # it's safer to do that here, as we have reassigned 
    # $col->{data} above.

    my %retvals;
    foreach ( @cols )
    {
      $_->{retinfo}{data} = $_->{data};
      $retvals{$_->{name}} = $_->{retinfo};
    }

    $retvals{_hdr} = $hdr if $opt{rethdr};
    return %retvals;
  }

  if ( $opt{rethash} )
  {
    my %retvals = map { $_->{name} => $_->{data} } @cols;
    $retvals{_hdr} = $hdr if $opt{rethdr};
    return %retvals;
  }

  # just return the data in the order they were requested.
  if ( wantarray() )
  {
    my @retvals = map { $_->{data} } @cols;
    unshift @retvals, $hdr if $opt{rethdr};
    return @retvals;
  }

  # if we're called in a scalar context, and there's but one column,
  # return the column directly.  always stick in the header, as a freebee
  if ( 1 == @cols )
  {
    my $pdl = $cols[0]->{data};
    tie my %hdr, 'Astro::FITS::Header', $hdr;
    $pdl->sethdr( \%hdr );
    return $pdl;
  }

  # scalar context, more than one column returned? doesn't make sense,
  # does it? we've tested for this before, but it doesn't hurt to stick
  # it here to remind us.

  croak( "rdfitsTable called in scalar context, but it read more than one column?\n" );

}

1;
