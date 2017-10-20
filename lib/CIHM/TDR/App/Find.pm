package CIHM::TDR::App::Find;

use common::sense;
use Data::Dumper;
use MooseX::App::Command;

extends qw(CIHM::TDR::App);

parameter 'uid' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[The uid of the AIP (In contributor.identifier or identifier form)],
);

option 'showpool' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Show which pool identifier was found in],
);

option 'contributor' => (
  is => 'rw',
  isa => 'Str',
  default => 'oocihm',
  documentation => q[Contributor to use if not supplied with uid],
);

option 'check' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Checks if AIPs exist (Display missing)],
);

option 'manifest' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Display manifest information],
);


command_short_description 'Finds path to bag in repository';
command_usage 'tdr find <uid> [<uid> ...] [long options...]';


sub run {
  my ($self) = @_;

  use CIHM::TDR;
  use CIHM::TDR::Repository;
  use Archive::BagIt;

  my $t_repo  = new CIHM::TDR::Repository($self->conf);

  my @uids = @{($self->extra_argv)[0]};
  unshift (@uids,$self->uid);

  foreach my $param_uid (@uids) {
      if (index($param_uid,".") == -1) {
	  $param_uid = $self->contributor . "." . $param_uid;
      }

      my $uid;
      # Convert UID on command line to simple UID (may be path)
      if (!($uid = ($t_repo->path_uid($param_uid))[2])) {
	  die "'$param_uid' is not a UID\n";
      }
      my ($contributor, $identifier) = split(/\./,$uid);
      my @aip = $t_repo->find_aip_pool($contributor, $identifier);

      if (@aip) {
	  next if ($self->check);
	  my $aippath = join("/",@aip[1 .. 4]);
	  if($aip[0] && $self->showpool) {
	      print $aippath . " (pool=".$aip[0].")\n";
	  } else {
	      print $aippath ."\n";
	  }
          if ($self->manifest) {
              print Data::Dumper->Dump([$t_repo->get_manifestinfo($aippath)], ["Manifest Information"])."\n";

          }
      } else {
	  print "$contributor.$identifier [MISSING]\n";
      }
  }
}

1;
