use 5.008002;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Astro::FITS::CFITSIO::Simple',
    VERSION_FROM      => 'lib/Astro/FITS/CFITSIO/Simple.pm', # finds $VERSION
    PREREQ_PM         => {
			  Params::Validate => 0.72,
			  Astro::FITS::CFITSIO => 1.02,
			  Astro::FITS::CFITSIO::CheckStatus => 0.03,
			  Astro::FITS::Header::CFITSIO,
			 }, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
     (
       AUTHOR         => 'Diab Jerius <djerius@cpan.org>') : ()),
);
