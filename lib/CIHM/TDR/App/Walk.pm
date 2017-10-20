package CIHM::TDR::App::Walk;

use common::sense;
use Data::Dumper;
use MooseX::App::Command;

extends qw(CIHM::TDR::App);

option 'import' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Import missing AIPs into database],
);

option 'update' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Update data for AIPs which exist but have mismatch],
);

option 'quiet' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Don't report on new or mismatched AIPs],
);

option 'quietupdate' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Don't report on database updates],
);


command_short_description 'Walks repository to find errors in structure, as well as new or missing AIPs';

sub run {
  my ($self) = @_;

  use CIHM::TDR;
  use CIHM::TDR::Repository;
  use Archive::BagIt;

  my $t_repo  = new CIHM::TDR::Repository($self->conf);
  $self->{t_repo}=$t_repo;

  if (!$t_repo->tdrepo) {
      say "Missing <tdrepo> configuration block in config\n";
      exit;
  }
  # Hash of the Found AIPs
  $self->{aipfound}={};

  # AIPs from database in {aiplist}
  $| = 1;
  print "Loading AIP list from database...";
  $self->get_aiplist();
  my $aipcount= keys $self->{aiplist};
  print " $aipcount found in DB.\n";

  $self->{aip_count}=0;

  my @pools = $t_repo->pools;
  foreach my $poolname (@pools) {
      $self->walk_pool($poolname);
  }

  my $aiplist= keys $self->{aiplist};
  if (! $self->quiet && ($aiplist > 0)) {
      print "There were $aiplist AIPs only found in DB:\n--begin--\n";
      foreach my $aipkey (keys $self->{aiplist}) {
          print "$aipkey\n";
      }
      print "--end--\n";
  }

  my $aipfound= keys $self->{aipfound};
  print $self->{aip_count} . " AIPs found on disk";
  if ($aipfound == $self->{aip_count}) {
      print ".\n";
  } else {
      print ", only $aipfound matched database.\n";
  }
}

sub walk_pool {
    my ($self,$poolname) = @_;
    my $t_repo  = $self->{t_repo};
    my $path =  $t_repo->aip_basepath($poolname);

    if (opendir (my $dh, $path)) {
        while(readdir $dh) {
            next if $_ eq "." || $_ eq "..";
            if ($t_repo->depositor_valid($_)) {
                $self->walk_depositor($poolname,$path,$_);
            } else {
                print STDERR "Depository $_ invalid at: $path/$_\n";
            }
        }
        closedir $dh;
    } else {
        print STDERR "Couldn't open $path\n";
        exit 1;
    }
}

sub walk_depositor {
    my ($self,$poolname,$path,$depositor) = @_;
    my $t_repo  = $self->{t_repo};

    if (opendir (my $dh, "$path/$depositor")) {
        while(readdir $dh) {
            next if $_ eq "." || $_ eq "..";
            if (/^\d\d\d$/) {
                $self->walk_hash($poolname,$path,$depositor,$_);
            } else {
                print STDERR "Hash $_ invalid at: $path/$depositor/$_\n";
            }
        }
        closedir $dh;
    } else {
        print STDERR "Couldn't open $path/$depositor\n";
        exit 1;
    }
}

sub walk_hash {
    my ($self,$poolname,$path,$depositor,$hash) = @_;
    my $t_repo  = $self->{t_repo};

    if (opendir (my $dh, "$path/$depositor/$hash")) {
        while(readdir $dh) {
            next if $_ eq "." || $_ eq "..";
            my ($depositordir, $identifier) = split(/\./,$_);
            if ($depositor eq $depositordir) {
                if ($t_repo->aip_valid($depositor,$identifier)) {
                    $self->found_aip($poolname,$path,$depositor,$hash,$identifier);
                } else {
                    print STDERR "AIP $_ invalid at: $path/$depositor/$hash/$_\n";
                }
            } else {
                print STDERR "Depositor in AIP ID didn't match depositor in path at: $path/$depositor/$hash/$_\n";
            }
        }
        closedir $dh;
    } else {
        print STDERR "Couldn't open $path/$depositor/$hash\n";
        exit 1;
    }
}


sub found_aip {
    my ($self,$poolname,$path,$depositor,$hash,$identifier) = @_;
    my $t_repo  = $self->{t_repo};
    my $aipid=$depositor.".".$identifier;
    my $aippath="$path/$depositor/$hash/$aipid";
    my $manifest = "$aippath/manifest-md5.txt";
    my ($statman,$manifestdate);
    my $update=0;

    if (-f $manifest) {
        my $updatedoc=$t_repo->get_manifestinfo($aippath,{ pool => $poolname });

        $self->{aip_count}++;
        if (exists $self->{aiplist}->{$aipid}) {
            if (! exists $self->{aiplist}->{$aipid}->{'manifest md5'}) {
                # Initialize variable -- will be noticed as mismatch, but without PERL error
                $self->{aiplist}->{$aipid}->{'manifest md5'}='[unset]';
            }
            if ( $self->{aiplist}->{$aipid}->{'manifest md5'} ne
                 $updatedoc->{'manifest md5'}) {
                if ($self->update) {
                    $update=1;
                }
                if (! $self->quiet) {
                    print "MD5 mismatch $aipid: ".$self->{aiplist}->{$aipid}->{'manifest md5'}." != ".$updatedoc->{'manifest md5'}."\n";
                }
            }
            if (! exists $self->{aiplist}->{$aipid}->{'manifest date'}) {
                # Initialize variable -- will be noticed as mismatch, but without PERL error
                $self->{aiplist}->{$aipid}->{'manifest date'}='[unset]';
            }
            if ( $self->{aiplist}->{$aipid}->{'manifest date'} ne
                 $updatedoc->{'manifest date'}) {
                if ($self->update) {
                    $update=1;
                }
                if (! $self->quiet) {
                    print "Date mismatch $aipid: ".$self->{aiplist}->{$aipid}->{'manifest date'}." != ".$updatedoc->{'manifest date'}."\n";
                }
            }
            if (! exists $self->{aiplist}->{$aipid}->{'pool'}) {
                # Initialize variable -- will be noticed as mismatch, but without PERL error
                $self->{aiplist}->{$aipid}->{'pool'}='[unset]';
            }
            if ( $self->{aiplist}->{$aipid}->{'pool'} ne
                 $updatedoc->{'pool'}) {
                if ($self->update) {
                    $update=1;
                }
                if (! $self->quiet) {
                    print "Pool mismatch $aipid: ".$self->{aiplist}->{$aipid}->{'pool'}." != ".$updatedoc->{'pool'}."\n";
                }
            }
            $self->{aipfound}->{$aipid}=$self->{aiplist}->{$aipid};
            delete $self->{aiplist}->{$aipid};
        } else {
            if (! $self->quiet) {
                print "New AIP found: $aipid\n";
            }
            my $updatedoc=$t_repo->get_manifestinfo($aippath,{});
            $self->{aipfound}->{$aipid}=$updatedoc;
            if ($self->import) {
                $update=1;
            }
        }
        if ($update) {
            if (! $self->quietupdate) {
                print "Updating: " . 
                    Data::Dumper->new([$updatedoc],[$aipid])->Dump . "\n";
            }
            $t_repo->update_item_repository($aipid,$updatedoc);
        }
    } else {
        print STDERR "Couldn't find $manifest\n";
    }
}

# This fills in the structure that has the AIP information for this
# repository from CouchDB
# Only used by Walk, so kept here rather than in CIHM::TDR::REST::tdrepo
sub get_aiplist {
    my $self = shift;
    my $res;
    my $t_repo  = $self->{t_repo};
    my $repository = $t_repo->{reponame};

    $res = $t_repo->tdrepo->get("/".$t_repo->tdrepo->database."/_design/tdr/_list/manifestinfo/tdr/repoown?reduce=false&startkey=[\"$repository\"]&endkey=[\"$repository\",{}]&include_docs=true",{}, {});
  if ($res->code == 200) {
      if ($res->failed) {
          print STDERR ("get_aip() failed flag set\n". $res->response->as_string() . "\n");
          exit 1;
      }
      if (! keys $res->data) {
          print STDERR ("get_aip() empty hash\n". $res->response->as_string() . "\n");
          exit 1;
      }
      $self->{aiplist}=$res->data;
  } else {
      print STDERR ("get_aiplist() GET return code: ".$res->code . "\nError: " . $res->error ."\n");
      exit 1;
  }
}

1;
