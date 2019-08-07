package CIHM::TDR::Swift;

use strict;
use warnings;

use Carp;
use CIHM::TDR::TDRConfig;
use CIHM::Swift::Client;
use CIHM::TDR::Repository;
use Data::Dumper;
use File::Find;


=head1 NAME

CIHM::TDR::Swift - Managing Canadiana style AIPs within Openstack Swift

=head1 SYNOPSIS

    my $t_repo = CIHM::TDR::Swift->new({config => $configpath});
      where $configpath is as defined in CIHM::TDR::TDRConfig

=cut

sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::TDR::Replication->new() not a hash\n";
    };
    $self->{args} = $args;

    $self->{tdr_repo} = new CIHM::TDR::Repository({
        configpath => $self->configpath
                                                  });
    my %confighash = %{CIHM::TDR::TDRConfig->instance($self->configpath)->get_conf};

    # Undefined if no <swift> config block
    if(exists $confighash{swift}) {
	my %swiftopt;
	foreach ("server","user","password","account") {
	    if (exists  $confighash{swift}{$_}) {
		$swiftopt{$_}=$confighash{swift}{$_};
	    }
	}
        $self->{swift}=CIHM::Swift::Client->new(%swiftopt);
	$self->{swiftconfig}=$confighash{swift};
    } else {
	croak "No <swift> configuration block in ".$self->configpath."\n";
    }

    # Undefined if no <tdrepo> config block
    if (exists $confighash{tdrepo}) {
        $self->{tdrepo} = new CIHM::TDR::REST::tdrepo (
            server => $confighash{tdrepo}{server},
            database => $confighash{tdrepo}{database},
            type   => 'application/json',
            conf   => $self->configpath,
            repository => $self->repository,
            clientattrs => {timeout => 3600},
            );
        $self->{tdrepo_updateadd}=exists $confighash{tdrepo}{updateadd};
    } else {
        croak "Missing <tdrepo> configuration block in config\n";
    }
    return $self;
}

# Simple accessors for now -- Do I want to Moo?
sub configpath {
    my $self = shift;
    return $self->{args}->{configpath};
}
sub swiftconfig {
    my $self = shift;
    return $self->{swiftconfig};
}
sub repository {
    my $self = shift;
    return $self->swiftconfig->{repository};
}
sub container {
    my $self = shift;
    return $self->swiftconfig->{container};
}
sub tdr_repo {
    my $self = shift;
    return $self->{tdr_repo};
}
sub tdrepo {
    my ($self) = shift;
    return $self->{tdrepo};
}
sub log {
    my $self = shift;
    return $self->tdr_repo->logger;
}
sub swift {
    my $self = shift;
    return $self->{swift};
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

    $self->log->info("Looking for replication work");
    $self->set_replicationwork({
        date => $self->since ,
        localdocument => $self->localdocument ,
                               });
}

sub set_replicationwork {
    my ($self, $params) = @_;
    my ($res, $code);

    my $newestaips = $self->tdrepo->get_newestaip($params);
    if (!$newestaips || !scalar(@$newestaips)) {
        # carp "Nothing new....";
        return;
    }
    foreach my $thisaip (@$newestaips) {
	my $aip = $thisaip->{key};
	my ($contributor, $identifier) = split(/\./,$aip);
	
	# Only set replication for AIPs which are on this repository
	if ($self->tdr_repo->find_aip_pool($contributor, $identifier)) {
	    my $priority = 5;  # Set default priority
	    my $match = $self->tdr_repo->aip_match($aip);
	    if ($match && $match->{replicate}) {
		$priority = $match->{replicate};
	    }
	    $self->tdrepo->update_item_repository($aip,{ replicate => $priority });
	}
    }
}


sub replicate {
    my ($self,$options) = @_;

    if (exists $options->{aip})  {
	$self->replicateaip($options->{aip});
    } else {
	my $txtopts;
	my $opts = { limit => 1};
	if (exists $options->{skip}) {
	    $opts->{skip}=$options->{skip};
	    $txtopts .= " skip=". $opts->{skip};
	}
	my $limit;
	if (exists $options->{limit}) {
	    $limit=$options->{limit};
	    $txtopts .= " limit=$limit";
	}
	$self->log->info("Replicate: $txtopts");
	# One by one, sorted by priority, get the AIPs we should replicate
	my @replicateaips;
	while ( ((! defined $limit) || $limit>0)
		&& (@replicateaips=$self->tdrepo->get_replicate($opts))
		&& scalar(@replicateaips)) {
	    $limit-- if $limit;
	    $self->replicateaip(pop @replicateaips);
	}
    }
}


sub replicateaip {
    my ($self,$aip) = @_;
    
    my ($contributor, $identifier) = split(/\./,$aip);
    my $aippath = $self->tdr_repo->find_aip_pool($contributor, $identifier);

    my $updatedoc = {
	# Whatever the outcome, if we update we mark the replication as done.
	replicate => "false"
    };
    
    if ($aippath) {
	$self->log->info("Replicating $aip");
	$updatedoc = $self->tdr_repo->get_manifestinfo($aippath);
	# Reset this, cleared by above call
	$updatedoc->{replicate}="false";
	
	my @aipfiles;
	find(
	    sub {-f && -r && push @aipfiles, $File::Find::name;},
	    $aippath
	    );

	foreach my $aipfile (@aipfiles) {
	    my $object = $aip . substr $aipfile, length $aippath;

	    open(my $fh, '<:raw', $aipfile)
		or die "replicate_aip: Could not open file '$aipfile' $!\n";

	    my $filedate="unknown";
	    my $mtime=(stat($fh))[9];
	    if ($mtime) {
		my $dt = DateTime->from_epoch(epoch => $mtime);
		$filedate = $dt->datetime. "Z";
	    }
	    $self->swift->object_put($self->container,$object, $fh, { 'File-Modified' => $filedate});
	    close $fh;
	}
    } else {
	carp "$aip not found\n";  # This shouldn't ever happen
    }
    
    # Inform database
    $self->tdrepo->update_item_repository($aip,$updatedoc);
}

1;
