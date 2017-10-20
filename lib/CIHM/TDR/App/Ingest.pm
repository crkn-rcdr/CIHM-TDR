package CIHM::TDR::App::Ingest;

use common::sense;
use Data::Dumper;
use Storable qw(freeze thaw);
use MooseX::App::Command;
use Try::Tiny;
use CIHM::TDR;
use Archive::BagIt;
use Digest::MD5;

extends qw(CIHM::TDR::App);

option 'update' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Allow update of an existing AIP],
);

option 'pool' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[Name of the pool to store the AIP (Default is pool with most available space)],
);

parameter 'contributor' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[The contributor for the SIP],
);

parameter 'sip_root' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[Path to the SIP to be ingested into AIP],
);

command_short_description 'Rebuild an AIP\'s CMR record';

sub run {
  my ($self) = @_;

  my $TDR = CIHM::TDR->new($self->conf);
  my $t_repo  = $TDR->{repo};

  my $sip_root=$self->sip_root;

  # Get the md5 of the metadata.xml to compare with all previous revisions.
  my $metadata=$sip_root."/data/metadata.xml";
  open(METADATA, "<", $metadata) or die "Can't open $metadata\n";
  binmode(METADATA);
  my $metadatamd5=Digest::MD5->new->addfile(*METADATA)->hexdigest;
  close(METADATA);

  my $sip = CIHM::TDR::SIP->new($sip_root);

  # Checks to determine if the environment is set correctly for ingest.
  $TDR->ingest_check($self->contributor, $sip->identifier,$self->update,$metadatamd5);

  eval { $TDR->ingest($self->contributor, $sip_root,$self->pool) };
  die("Ingest failed: $@\n") if $@;
}

1;
