package CIHM::TDR::Replication;

use strict;
use Carp;
use CIHM::TDR::TDRConfig;
use CIHM::TDR::Repository;
use CIHM::TDR::ContentServer;
use Archive::BagIt::Fast;
use Try::Tiny;
use File::Spec;
use File::Path qw(make_path remove_tree);

=head1 NAME

CIHM::TDR::Replication - TDR Repository AIP replication

=head1 SYNOPSIS

    my $replication = CIHM::TDR::Replication->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is as defined in CIHM::TDR::TDRConfig

=cut

sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::TDR::Replication->new() not a hash\n";
    };
    $self->{args} = $args;

    $self->{config} = CIHM::TDR::TDRConfig->instance($self->configpath);

    $self->{tdr_repo} = new CIHM::TDR::Repository({
        configpath => $self->configpath
                                                  });
    $self->{incoming} = $self->tdr_repo->incoming_basepath();

    if (!$self->tdr_repo->tdrepo) {
        croak "Missing <tdrepo> configuration block in config\n";
    }
    $self->{cserver} = new CIHM::TDR::ContentServer($self->configpath);
    if (!$self->cserver) {
	croak STDERR "Missing ContentServer configuration.\n";
    }
    return $self;
}

# Simple accessors for now -- Do I want to Moo?
sub configpath {
    my $self = shift;
    return $self->{args}->{configpath};
}
sub config {
    my $self = shift;
    return $self->{config};
}
sub incoming {
    my $self = shift;
    return $self->{incoming};
}
sub tdr_repo {
    my $self = shift;
    return $self->{tdr_repo};
}
sub cserver {
    my $self = shift;
    return $self->{cserver};
}
sub log {
    my $self = shift;
    return $self->{tdr_repo}->logger;
}
sub tdrepo {
    my $self = shift;
    return $self->tdr_repo->tdrepo;
}
sub since {
    my $self = shift;
    return $self->{args}->{since};
}
sub localdocument {
    my $self = shift;
    return $self->{args}->{localdocument};
}

sub replicationwork {
    my ($self) = @_;

    my @test = $self->cserver->replication_repositories();
    if (!@test || !scalar(@test)) {
	print STDERR "No replication repositories (Repositories which have listed a priority)....\n";
	exit 1;
    }
    $self->log->info("Looking for replication work");
    $self->set_replicationwork({
        date => $self->since ,
        localdocument => $self->localdocument ,
        repos => \@test
                               });
}

sub replicate {
    my ($self) = @_;

    my @rrepos = $self->cserver->replication_repositories();
    if (!@rrepos || !scalar(@rrepos)) {
	print STDERR "No replication repositories (Repositories which have listed a priority)....\n";
	exit 1;
    }
    
    $self->log->info("Replicate");

    my $incoming=$self->incoming;
    if ($incoming) {
        $self->scan_incoming($incoming);
    }

    # One by one, sorted by priority, get the AIPs we should replicate
    my @replicateaips;
    my $lastid='';
    while ( (@replicateaips=$self->tdrepo->get_replicate({ limit => 1}))
	    && scalar(@replicateaips)) {
	my $thisid=pop @replicateaips;
	if ($thisid ne $lastid) {
	    $self->replicateaip($thisid);
	    $lastid=$thisid;
	} else {
	    $self->log->info("get_replicate returned $lastid again...");
	}
    }

}

sub scan_incoming {
    my ($self,$incoming) = @_;

    if (opendir (my $dh, $incoming)) {
        while(readdir $dh) {
            next if $_ eq "." || $_ eq ".." || $_ eq "lost+found";
            my $aip = $_;
            my $path = File::Spec->catfile($incoming,$aip);
            if (-d $path) {
                if (-f $path."/manifest-md5.txt") {
                    my $verified;
                    try {
                        my $bagit = new Archive::BagIt::Fast($path);
                        $verified = $bagit->verify_bag();
                    };
                    if ($verified) {
                        $self->log->info("Found valid AIP at $path");
                        my $updatedoc = $self->updatedoc($aip);
                        my $aipinfo = $self->tdr_repo->get_manifestinfo($path);
                        if ($aipinfo->{'manifest md5'} && 
                            $updatedoc->{'manifest md5'} && 
                            $aipinfo->{'manifest md5'} eq $updatedoc->{'manifest md5'}) {
                            $self->log->info("$aip with md5(". $updatedoc->{'manifest md5'} . ") already exists.");
                            $self->tdrepo->update_item_repository($aip,$updatedoc);
                        } else {
                            $self->rsyncadd($aip,$path,$updatedoc);
                        }
                        $self->log->info("Cleaning up $path");
                        remove_tree($path);
                    } else {
                        $self->log->info("Found invalid AIP at $path");
                    }
                } else {
                    $self->log->info("Found non-AIP directory $path");
                }
            } else {
                $self->log->info("Found non-directory $path");
            }
        }
        closedir $dh;
    } else {
        $self->log->error("Couldn't open $incoming");
    }
}


# Returns a fresh update document. If the AIP exists in the repository, the
# 'manifest md5' and 'manifest date' fields will be filled in.
sub updatedoc {
    my ($self,$aip) = @_;
    my ($contributor, $identifier) = split(/\./,$aip);

    my $updatedoc = {};

    # First check if the UID is already here...
    my @found = $self->tdr_repo->find_aip_pool($contributor,$identifier);
    if (@found) {
        my $aip_path = join("/",@found[1 .. 4]);
        $updatedoc = $self->tdr_repo->get_manifestinfo($aip_path);
        # Don't forget to set the pool
        $updatedoc->{pool}=$found[0];
    }
    return $updatedoc;
}


# Note that replicate and `tdr add` have similarities, but logic different
# enough to not simply use common subroutine.
sub replicateaip {
    my ($self,$aip) = @_;
    my ($contributor, $identifier) = split(/\./,$aip);

    $self->log->info("Replicating $aip");

    my $updatedoc = $self->updatedoc($aip);

    # Whatever the outcome, if we update we mark the replication as done.
    $updatedoc->{replicate}="false";

    my $hasnew = $self->tdrepo->get_newestaip({ keys => [$aip]});
    if (!$hasnew) {
        # We got an HTTP error code, so exit....
        exit 1;
    }
    if (! scalar(@$hasnew)) {
        # Maybe we missed filling in the manifest information?
        # Otherwise increase priority to move AIP from being 'next' again.
        if (!exists $updatedoc->{'manifest md5'}) {
            # TODO: Incrementally update priority to 9 before going to
            # letter....
            $updatedoc->{priority}="a";
            $self->log->warn("Can't find source ContentServer for $aip");
        } else {
            # TODO:  Compare files on disk to possible existing database
            $self->log->info("$aip already existed on server");
        }
        $self->tdrepo->update_item_repository($aip,$updatedoc);
        return;
    }
    my @repos = @{$hasnew->[0]->{value}[1]};

    my @rrepos = $self->cserver->replication_repositories();
    my $copyrepo;
    # Loop though repos we would be willing to sync from
  REPOL: foreach my $fromrepo (@rrepos) {
      foreach my $thisrepo (@repos) {
          if ($thisrepo eq $fromrepo) {
              $copyrepo=$thisrepo;
              last REPOL;
          }
      }
  }
    if (!$copyrepo) {
        # This won't happen in regular operation, but might happen if replication topology changed.
        $self->log->info("Couldn't find repo to copy $aip");

        # Not a serious problem.  Update document to no longer be replicated, and go on to the next.
        $self->tdrepo->update_item_repository($aip,$updatedoc);
        return;
    }
    my $aipinfo=$self->cserver->get_aipinfo($aip,$copyrepo);
    if (!$aipinfo) {
        print STDERR "Couldn't get AIP information from $copyrepo\n";
        exit 1;        
    }
    if ($aipinfo->{'manifest md5'} && $updatedoc->{'manifest md5'}
        && $aipinfo->{'manifest md5'} eq $updatedoc->{'manifest md5'}) {
        print STDERR "$aip with md5(". $updatedoc->{'manifest md5'} . ") already exists.\n";
        $self->tdrepo->update_item_repository($aip,$updatedoc);
        return;
    }
    if (!$aipinfo->{'rsyncpath'}) {
        # This should never happen, unless something misconfigured
        print STDERR "Couldn't get rsyncpath for $aip from $copyrepo\n";
        exit 1;
    }

    $self->rsyncadd($aip,$aipinfo->{'rsyncpath'},$updatedoc);
}

# Note that rsyncadd and `tdr add` have similarities, but logic different
# enough to not simply use common subroutine.
sub rsyncadd {
    my ($self,$aip,$rsyncpath,$updatedoc) = @_;
    my ($contributor, $identifier) = split(/\./,$aip);

    my $pool=$self->tdr_repo->pool_free;
    if (!$pool) {
        # This should never happen, unless something misconfigured
        print STDERR "Couldn't get pool with free space\n";
        exit 1;
    }

    $self->log->info("Rsync from $rsyncpath");
    $self->tdr_repo->incoming_rsync($pool,$aip,$rsyncpath);
    my $incomingpath = $self->tdr_repo->find_incoming_pool($contributor,$identifier);
    if (! $incomingpath ) {
        # We couldn't manage to copy it...
        my $errmessage="Error with rsync from $rsyncpath for $aip";
        print STDERR $errmessage."\n";
        $self->log->warn($errmessage);
        # Set priority to letter, which keeps in _view/replicate, but won't be part of replication
        $updatedoc->{priority}="a";
        $self->tdrepo->update_item_repository($aip,$updatedoc);
        return;
    }
    my $verified;
    try {
        my $bagit = new Archive::BagIt::Fast($incomingpath);
        my $valid = $bagit->verify_bag();
        # If we have the size, then set it in database
        if ($bagit->{stats} && $bagit->{stats}->{size}) {
            $updatedoc->{'filesize'}=$bagit->{stats}->{size};
        }
        $verified = $valid;
    };
    if (!$verified) {
        # Bag wasn't valid.
        my $errmessage = "Error verifying bag: $incomingpath";
        print STDERR $errmessage."\n";
        $self->log->warn($errmessage);

        # Set priority to letter, which keeps in _view/replicate , but won't be part of replication
        $updatedoc->{priority}="a";
        $updatedoc->{filesize}="";
        $self->tdrepo->update_item_repository($aip,$updatedoc);
        exit;
    }
    $self->log->info("adding ".$incomingpath);

    # If the AIP already exists in the repository....
    if (exists $updatedoc->{'manifest md5'}) {
        if(!($self->tdr_repo->aip_delete($contributor,$identifier))) {
            my $errmessage="Failed to remove AIP: $aip";
            print STDERR $errmessage."\n";
            $self->log->warn($errmessage);

            # Set priority to letter, which keeps in _view/replicate , but won't be part of replication
            $updatedoc->{priority}="a";
            $updatedoc->{filesize}="";
            $self->tdrepo->update_item_repository($aip,$updatedoc);
            exit;
        }
    }
    if(!($self->tdr_repo->aip_add($contributor,$identifier,$updatedoc))) {
        my $errmessage="Failed to add AIP: $aip";
        print STDERR $errmessage."\n";
        $self->log->warn($errmessage);

        # Set priority to letter, which keeps in _view/replicate , but won't be part of replication
        $updatedoc->{priority}="a";
        $updatedoc->{filesize}="";
        $self->tdrepo->update_item_repository($aip,$updatedoc);
        exit;
    }
}

sub set_replicationwork {
    my ($self, $params) = @_;
    my ($res, $code);

    my $newestaips = $self->tdrepo->get_newestaip($params);
    if (!$newestaips || !scalar(@$newestaips)) {
        # print STDERR "Nothing new....";
        return;
    }
    # Loop through all the changed AIPs to see if the AIP is on
    # the machine I want to replicate from
    my @myrepos = @{$params->{repos}};
    foreach my $thisaip (@$newestaips) {
        my $aip = $thisaip->{key};
        my @repos = @{$thisaip->{value}[1]};
        # Loop though repos we would be willing to sync from
        my $foundit;
        REPOL: foreach my $fromrepo (@myrepos) {
            foreach my $thisrepo (@repos) {
                if ($thisrepo eq $fromrepo) {
                    $foundit=1;
                    last REPOL;
                }
            }
        }
        if ($foundit) {
            # We found it, so set it to be replicated.
            my $priority = 5;  # Set default priority
            my $match = $self->tdr_repo->aip_match($aip);
            if ($match && $match->{replicate}) {
                $priority = $match->{replicate};
            }
            $self->tdrepo->update_item_repository($aip,{ replicate => $priority });
        }
    }
}

1;
