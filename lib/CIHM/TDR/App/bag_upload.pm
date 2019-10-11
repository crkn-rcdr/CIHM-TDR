package CIHM::TDR::App::bag_upload;

use common::sense;
use Data::Dumper;
use Storable qw(freeze thaw);
use MooseX::App::Command;
use Try::Tiny;
use CIHM::TDR::Swift;
use Archive::BagIt;

extends qw(CIHM::TDR::App);

parameter 'sourcedir' => (
    is => 'rw',
    isa => 'Str',
    required => 1,
    documentation => q[Source directory (bag format)],
    );

parameter 'uid' => (
    is => 'rw',
    isa => 'Str',
    required => 1,
    documentation => q[The uid of the AIP (In contributor.identifier form)],
    );

option 'tries' => (
    is => 'rw',
    isa => 'Int',
    default => 3,
    documentation => q[Number of times to retry before failing],
    );


command_short_description 'Copy AIP to Swift';

sub run {
  my ($self) = @_;

  my $swift = CIHM::TDR::Swift->new({ configpath => $self->conf });

  die "No <swift> configuration\n" if ! $swift;
  
  die "'".$self->sourcedir."' is not a directory\n" if (! -d $self->sourcedir);

  # Try to copy 3 times before giving up.
  my $success=0;
  for (my $tries=$self->tries ; ($tries > 0) && ! $success ; $tries --) {
      try {
	  $swift->bag_upload($self->sourcedir,$self->uid);
	  $success=1;
      };
  }
  if ($success) {
      print "Success!\n";
  } else {
      print "Failed!\n";
  }
}

1;
