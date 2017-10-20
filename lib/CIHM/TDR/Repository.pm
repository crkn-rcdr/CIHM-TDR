package CIHM::TDR::Repository;

use strict;
use Config::General;
use Data::Dumper;
use Log::Log4perl;
use Try::Tiny;
use File::Spec;
use File::Path qw(make_path remove_tree);
use File::Basename;
use String::CRC32;
use CIHM::TDR::TDRConfig;
use CIHM::TDR::REST::tdrepo;
use Filesys::Df;
use Digest::MD5;
my  $DEBUG=0;

=head1 NAME

CIHM::TDR::Repository - TDR Repository manipulation

=head1 SYNOPSIS

    my $t_repo = CIHM::TDR::Repository->new($configpath);
      where $configpath is as defined in CIHM::TDR::TDRConfig

    $path = $t_repo->aip_basepath($pool)
      Returns the full path to an AIP directory.  Returns the AIP dirctory
      of the provided pool, or if pool not provided the AIP directory
      of the main repository (Potentially a read-only union mount).

    $hash = $t_repo->aip_hash($contributor, $identifier);
      Returns the hash used to build the pathname to an AIP within the TDR.

TODO: Fill in other subroutines once stabilized.
=cut

sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    my $tdr_configpath;
    my $argtype=ref($args);
    if (!$argtype) {
	$tdr_configpath=$args;
    } elsif (ref($args) eq "HASH") {
	$tdr_configpath=$args->{configpath};
    } else {
	die "Don't understand that type of argument: $argtype\n";
    };

    $self->{config_path} = $tdr_configpath;
    $self->{config} = CIHM::TDR::TDRConfig->instance($tdr_configpath);
    $self->{logger} = $self->{config}->logger;

    # Confirm there is a named repository block in the config
    my %confighash = %{$self->{config}->get_conf};
    if (! (%confighash && $confighash{'repository'})) {
	die "No <repository> block in configuration file!\n";
    }
    my @repos = keys %{ $confighash{'repository'}};

    # If the name was missing, then all the other config options become keys.
    # Only one block allowed, so using only one key as way to differentiate.
    if (scalar @repos != 1) {
	die "The <repository> block must be named!\n";       
    }

    # Set up name (the key) for this repository config, with the value
    # being all the configuration options within the block
    $self->{reponame} = shift @repos;
    $self->{repoconf} = $confighash{'repository'}->{$self->{reponame}};

    # Any <depositor> blocks
    $self->{depositors} = $confighash{'depositor'};

    # The <aipmatch> block
    if (exists $confighash{'aipmatch'}) {
        $self->{aipmatch} = $confighash{'aipmatch'};
    }

    # Undefined if no <tdrepo> config block
    if (exists $confighash{tdrepo}) {
        $self->{tdrepo} = new CIHM::TDR::REST::tdrepo (
            server => $confighash{tdrepo}{server},
            database => $confighash{tdrepo}{database},
            type   => 'application/json',
            conf   => $tdr_configpath,
            repository => $self->{reponame},
            clientattrs => {timeout => 3600},
            );
        $self->{tdrepo_updateadd}=exists $confighash{tdrepo}{updateadd};
    }
    return $self;
}

# Accessors -- Not yet a need to Moo or Class::Accessor yet..
sub tdrepo {
    my ($self) = shift;
    return $self->{tdrepo};
}
sub tdrepo_updateadd {
    my ($self) = shift;
    return $self->{tdrepo_updateadd};
}
sub logger {
    my ($self) = shift;
    return $self->{logger};
}
sub config {
    my ($self) = shift;
    return $self->{config};
}


# Return the configuration block for this repository
sub conf_repo {
  my $self = shift;
  return (%{$self->{repoconf}});
}

# Return the name given for the configuration block
sub conf_repo_name {
  my $self = shift;
  return ($self->{reponame});
}

sub  aip_basepath {
  my ($self,$pool) = @_;
  my %repo = $self->conf_repo;
  if ($pool) {
      my %pools = $self->conf_pools;
      if (!%pools) {
	  return;
      }
      my %mypool = %{$pools{$pool}};
      if (!%mypool || !$mypool{'aip'}) {
	  return;
      }
      return File::Spec->rel2abs($mypool{'aip'},
            $repo{'basepath'}."/".$mypool{'mountpoint'});
  } else {
      if ($repo{'aip'} && $repo{'basepath'}) {
	  return File::Spec->rel2abs($repo{'aip'},$repo{'basepath'});
      } else {
	  return;
      }
  }
}

sub  aip_rsyncpath {
  my ($self,$pool) = @_;
  my %repo = $self->conf_repo;
  if ($pool) {
      my %pools = $self->conf_pools;
      if (!%pools) {
	  return;
      }
      my %mypool = %{$pools{$pool}};
      if (!%mypool) {
	  return;
      }
      return $mypool{'rsyncpath'};
  }
}

sub  aip_cifspath {
  my ($self,$pool) = @_;
  my %repo = $self->conf_repo;
  if ($pool) {
      my %pools = $self->conf_pools;
      if (!%pools) {
	  return;
      }
      my %mypool = %{$pools{$pool}};
      if (!%mypool) {
	  return;
      }
      return $mypool{'cifspath'};
  }
}

sub incoming_basepath {
  my ($self,$pool) = @_;
  my %repo = $self->conf_repo;
  if ($pool) {
      my %pools = $self->conf_pools;
      if (!%pools) {
	  return;
      }
      my %mypool = %{$pools{$pool}};
      if (!%mypool || !$mypool{'incoming'}) {
	  return;
      }
      return File::Spec->rel2abs($mypool{'incoming'},
            $repo{'basepath'}."/".$mypool{'mountpoint'});
  } else {
      if ($repo{'incoming'}) {
          return File::Spec->rel2abs($repo{'incoming'},$repo{'basepath'});
      }
  }
}

# Returns a path for where an AIP would be stored in incoming
# (Directory may or may not already exist -- test first if this matters)
sub incoming_aippath { 
  my ($self,$poolname,$aip) = @_;
  return $self->incoming_basepath($poolname) . "/$aip";
}

sub trashcan_basepath {
  my ($self,$pool) = @_;
  my %repo = $self->conf_repo;
  if ($pool) {
      my %pools = $self->conf_pools;
      if (!%pools) {
	  return;
      }
      my %mypool = %{$pools{$pool}};
      if (!%mypool || !$mypool{'trashcan'}) {
	  return;
      }
      return File::Spec->rel2abs($mypool{'trashcan'},
            $repo{'basepath'}."/".$mypool{'mountpoint'});
  }
}

# Returns a path for where an AIP would be stored in trashcan
# (Directory may or may not already exist -- test first if this matters)
sub trashcan_aippath { 
  my ($self,$poolname,$aip) = @_;
  return $self->trashcan_basepath($poolname) . "/$aip";
}

sub conf_pools {
  my $self = shift;

  my %repo = $self->conf_repo;
  my %pools;
  if(defined $repo{'pool'}) {
    %pools = %{$repo{'pool'}};
  }
  return (%pools);
}

# Return an array listing the names of the pools
sub pools {
  my $self = shift;
  my %pools = $self->conf_pools;
  return (keys %pools);
}

# Return an array listing the names of the read/write pools
sub pools_rw {
  my $self = shift;
  my %pools = $self->conf_pools;
  my @rwpools = ();
  if(%pools) {
      foreach my $poolname (keys %pools) {
	      my %mypool = %{$pools{$poolname}};
	      if (%mypool && $mypool{'incoming'} 
		  && ($mypool{'mode'} ne "ro")) {
		  push(@rwpools,$poolname);
	      }
      }
  }
  return @rwpools;
}

sub firstpool {
  my $self = shift;
  my @pools = $self->pools;
  if (@pools) {
      return $pools[0];
  }
}

sub pool_valid {
  my ($self,$pool) = @_;
  my %pools = $self->conf_pools;
  return defined $pools{$pool};
}

sub pool_free {
  my $self = shift;
  my @rwpools = $self->pools_rw;

  my $fpool;
  my $bavail=0;
  foreach my $pool (@rwpools) {
      my $ref = df($self->incoming_basepath($pool));
      if(defined($ref) && ($ref->{bavail} > $bavail)) {
	  $fpool=$pool;
	  $bavail=$ref->{bavail};
      }
  }
  return $fpool;
}

sub aip_valid {
    my($self, $depositor, $identifier) = @_;

    my %depositors = $self->conf_depositors;

    if (%depositors) {
        # If we have a list of valid depositors, use that
        return 0 unless (exists $depositors{$depositor});
    } else {
        # Otherwise, just check if it matches the pattern of characters
        return 0 unless ($depositor =~ /^[a-z]+$/);
    }

    # Check if the identifier matches the spec
    return 0 unless ($identifier =~ /^[A-Za-z0-9_]{5,64}$/);
    return 1;
}
# The <depositor> sections of the config
sub conf_depositors {
  my $self = shift; 
  if ($self->{depositors}) {
      return (%{$self->{depositors}});
  }
  return;
}

# Return an array listing the identifiers of the depositors
sub depositors {
  my $self = shift;
  my %depositors = $self->conf_depositors;
  return (keys %depositors);
}

# Return boolean if depositors in list
sub depositor_valid {
  my ($self,$depositor) = @_;
  my %depositors = $self->conf_depositors;
  return (exists $depositors{$depositor});
}

# Return the AIP hash for the specified $contributor and $identifier.
# Calls die() if parameters invalid, so check with aip_valid() first.
sub aip_hash {
    my($self, $contributor, $identifier) = @_;
    die("Contributor code '$contributor' contains invalid characters") unless ($contributor =~ /^[a-z]+$/);
    die("Identifier '$identifier' contains invalid characters") unless ($identifier =~ /^[A-Za-z0-9_]{5,64}$/);
    my $uid = "$contributor.$identifier";
    my $hashcode = substr(crc32($uid), -3);
    return $hashcode;
}

# Returns the pathname (or path components) for the specified
# $contributor and $identifier.
sub find_aip_pool {
    my($self, $contributor, $identifier) = @_;
    my $hashcode = $self->aip_hash($contributor, $identifier);
    my %pools = $self->conf_pools;
#             pool,basepath,contributor,hash,UUID
    my @aip =(undef,undef,$contributor,$hashcode,"$contributor.$identifier");
    my $aippath;
    my $found;
    my $basepath;

    if(%pools) {
	foreach my $poolname (keys %pools) {
	    $aip[1]=$self->aip_basepath($poolname);
	    $aippath = join("/",@aip[1 .. 4]);
	    $found = -d $aippath;
	    if ($found) {
		$aip[0]=$poolname;
		last;
	    }
	}
    }
    if (!$found) {
	$aip[1]=$self->aip_basepath();
	$aippath = join("/",@aip[1 .. 4]);
	$found = -d $aippath;
    }
    return if (!$found);
    return(@aip) if (wantarray);
    return $aippath;
}

# Takes a UID (or a full path that ends in a UID), and converts to
# a TDR path (contributor/hash/uid) or to an array (contributor,hash,uid)
sub path_uid {
    my($self, $uid) = @_;

    # Pattern based on aip_valid()
    if ($uid =~ /[\/]*([a-z]+)\.([A-Za-z0-9_]{5,64})[\/]*$/) {
	my $contributor = $1;
	my $identifier = $2;
	my $hashcode = $self->aip_hash($contributor, $identifier);
	my @aip =($contributor,$hashcode,"$contributor.$identifier");
	my $aippath = join("/",@aip);
	return(@aip) if (wantarray);
	return $aippath;
    }
}

# Returns the pathname (or path components) for the specified $contributor
# and $identifier within the incoming
sub find_incoming_pool {
    my($self, $contributor, $identifier) = @_;
    my %pools = $self->conf_pools;
#             pool,incoming path,,UUID
    my @aip =(undef,undef,"$contributor.$identifier");
    my $aippath;
    my $found;
    my $incomingpath;

    if(%pools) {
	foreach my $poolname (keys %pools) {
	    $aip[1]=$self->incoming_basepath($poolname);
	    $incomingpath = join("/",@aip[1 .. 2]);
	    $found = -d $incomingpath;
	    if ($found) {
		$aip[0]=$poolname;
		last;
	    }
	}
    }
    return if (!$found);
    return(@aip) if (wantarray);
    return $incomingpath;
}

# Returns the pathname (or path components) for the specified
# $contributor and $identifier within the trashcan
sub find_trash_pool {
    my($self, $contributor, $identifier) = @_;
    my %pools = $self->conf_pools;
#             pool,incoming path,,UUID
    my @aip =(undef,undef,"$contributor.$identifier");
    my $aippath;
    my $found;
    my $trashpath;

    if(%pools) {
	foreach my $poolname (keys %pools) {
	    $aip[1]=$self->trashcan_basepath($poolname);
	    $trashpath = join("/",@aip[1 .. 2]);
	    $found = -d $trashpath;
	    if ($found) {
		$aip[0]=$poolname;
		last;
	    }
	}
    }
    return if (!$found);
    return(@aip) if (wantarray);
    return $trashpath;
}


sub aip_delete {
    my($self, $contributor, $identifier) = @_;
    my @aip;
    while (@aip =  $self->find_aip_pool($contributor,$identifier)) {
	my $aippath = join("/",@aip[1 .. 4]);
	my $trashpath=$self->trashcan_basepath($aip[0])."/$contributor.$identifier";
	return if -d $trashpath;
	rename($aippath,$trashpath) or die ("can't rename bag: $aippath -> $trashpath $! : $@");
	# Update mtime, so time-based trashcan cleanup can work well.
	utime(undef,undef,$trashpath);
    }
    return 1;
}

sub aip_add {
    my($self, $contributor, $identifier, $updatedoc) = @_;
    my $hashcode = $self->aip_hash($contributor, $identifier);
    my @aip = $self->find_incoming_pool($contributor,$identifier);
    return if (!@aip);

    if (!$updatedoc) {
        $updatedoc = {};
    }

    my $pool = $aip[0];
    my $incomingpath=$aip[1] . "/" . $aip[2];
    my $aip_basepath=$self->aip_basepath($pool);
    my $aippath = "$aip_basepath/$contributor/$hashcode/$contributor.$identifier";

    if (-d $aippath) {
	die "Can't add aip: $aippath already exists!";
    }
    make_path(dirname($aippath));
    rename($incomingpath, $aippath) or die ("can't rename bag: $incomingpath -> $aippath $! : $@");

    $self->aip_add_db($contributor,$identifier,$updatedoc);
    return 1;
}

# Get a hash with the manifest md5 and mtime
# (In format that CouchDB accepts)
sub get_manifestinfo {
    my($self, $path, $manifestdoc) = @_;
    my $manifestpath=$path."/manifest-md5.txt";

    if (!$manifestdoc) {
        $manifestdoc = {};
    }
    open(MANIFEST, "<", $manifestpath) or die "Can't open manifest at $manifestpath\n";
    binmode(MANIFEST);
    $manifestdoc->{'manifest md5'}=Digest::MD5->new->addfile(*MANIFEST)->hexdigest;
    close(MANIFEST);
    my $mtime=(stat($manifestpath))[9];
    if ($mtime) {
        my $dt = DateTime->from_epoch(epoch => $mtime);
        $manifestdoc->{'manifest date'} = $dt->datetime. "Z";
    }
    return $manifestdoc;
}

# Adds or updates information about the AIP in the database(s)
# Note: There is no aip_delete_db() on purpose.  AIPs aren't deleted,
# just directories moved out of way as part of update process.
sub aip_add_db {
    my($self, $contributor, $identifier, $updatedoc) = @_;

    if (!$self->tdrepo) {
        # There are no databases configured
        return;
    }
    if (!$self->tdrepo_updateadd) {
        # Flag that indicates database should be updated on add not set
        return;
    }
    if (!$updatedoc) {
        $updatedoc = {};
    }
    my @aip = $self->find_aip_pool($contributor, $identifier) or die "Can't find AIP $contributor.$identifier\n";

    $updatedoc=$self->get_manifestinfo(join("/",@aip[1 .. 4]),$updatedoc);
    $updatedoc->{'pool'}=$aip[0];

    if ($self->tdrepo) {
        $self->tdrepo->update_item_repository("$contributor.$identifier",$updatedoc);
	$self->{logger}->debug ("updated bag in CouchDB: $contributor.$identifier");
    }
}

# If we have a couch config, update the doc, otherwise do nothing.
sub update_item_repository {
    my($self, $uid, $updatedoc) = @_;

    if ($self->tdrepo) {
        $self->tdrepo->update_item_repository($uid,$updatedoc);
    }
}

# 
sub incoming_rsync { 
  my ($self,$poolname,$aip,$source) = @_;
  if (! $self->pool_valid($poolname)) {
      die "Pool $poolname is not valid\n";
  }
  my $aippath = $self->incoming_basepath($poolname) . "/$aip";
  $source =~ s/\/*$//;

  mkdir $aippath;
  # Don't preserve owner or group, so run as intended user.
  my @rsynccmd=("rsync","-rlpt","--del","--partial","--timeout=10","$source/.","$aippath/.");

  $self->{logger}->debug ("Add --rsync: @rsynccmd");

  my $rsyncexit = 30;
  # https://download.samba.org/pub/rsync/rsync.html
  # 10 - Error in socket I/O
  # 12 - Error in rsync protocol data stream
  # 30 - Timeout in data send/receive
  while ($rsyncexit == 10 || $rsyncexit == 12 ||  $rsyncexit == 30 ) {
      system(@rsynccmd);
      if ($? == -1) {
          print "@rsynccmd\nfailed to execute: $!\n";
          return;
      }
      elsif ($? & 127) {
          printf "@rsynccmd\nchild died with signal %d, %s coredump\n",
          ($? & 127),  ($? & 128) ? 'with' : 'without';
          return;
      }
      $rsyncexit =  $? >> 8;
  }
  if ($rsyncexit) {
      printf "@rsynccmd\nchild exited with value $rsyncexit\n";
  }
}

# Given an AIP, loop through the patterns in the config to return the
# first matching config block
sub aip_match {
  my ($self,$aip) = @_;

  if ($self->{'aipmatch'}) {
      my $hash=$self->{'aipmatch'};
      my @order = split(' ',$hash->{'order'});

      foreach my $pattern (@order) {
          my $patternhash=$hash->{'pattern'}->{$pattern};
          my $exp = $patternhash->{'exp'};
          if ($aip =~ m/$exp/) {
              return $patternhash;
          }
      }
  }
  return;
}


1;
