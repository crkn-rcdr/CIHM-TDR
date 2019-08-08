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
	my $txtopts='';
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
	    $self->replicateaip(pop @replicateaips,$options);
	}
    }
}


sub replicateaip {
    my ($self,$aip,$options) = @_;
    
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
	my $validate = $self->validateaip($aip,$options);
	if ($validate->{'validate'}) {
	    if ($updatedoc->{'manifest date'} ne $validate->{'manifest date'}) {
		carp "Manifest Date Mismatch: ".$updatedoc->{'manifest date'}." != ".$validate->{'manifest date'}."\n"; # This shouldn't ever happen
	    }
	    if ($updatedoc->{'manifest md5'} ne $validate->{'manifest md5'}) {
		carp "Manifest MD5 Mismatch: ".$updatedoc->{'manifest md5'}." != ".$validate->{'manifest md5'}."\n"; # This shouldn't ever happen
	    }
	}
    } else {
	carp "$aip not found\n";  # This shouldn't ever happen
    }
    
    # Inform database
    $self->tdrepo->update_item_repository($aip,$updatedoc);
}

sub validate {
    my ($self,$options) = @_;

    if (exists $options->{aip})  {
	$self->validateaip($options->{aip},$options);
    } else {

	my $aiplistresp = $self->swift->container_get($self->container, {
	    delimiter => "/"
						      });
	if ($aiplistresp->code != 200) {
	    croak "container_get(".$self->container.") returned ". $aiplistresp->code . " - " . $aiplistresp->message. "\n";
	};
	foreach my $subdir (@{$aiplistresp->content}) {
	    my $aip = $subdir->{subdir};
	    chop($aip);
	    my $val = $self->validateaip($aip,$options);
	    if ($val->{validate}) {
		$self->log->info("verified Swift AIP: $aip");
	    } else {
		$self->log->warn("invalid Swift AIP: $aip");
	    }
	}
    }
}

sub validateaip {
    my ($self,$aip,$options) = @_;

    my $verbose = exists $options->{'verbose'};
    
    # Assume validated unless problem found
    my %return = (
	"validate" => 1,
	"filesize" => 0
	);

    my $aipdataresp = $self->swift->container_get($self->container, {
	prefix => $aip."/data/"
					      });
    if ($aipdataresp->code != 200) {
	croak "container_get(".$self->container.") returned ". $aipdataresp->code . " - " . $aipdataresp->message. "\n";
    };
    my %aipdata;
    foreach my $object (@{$aipdataresp->content}) {
	my $file=substr $object->{name},(length $aip)+1;
	$aipdata{$file}=$object;
    }
    undef $aipdataresp;

    
    my $manifest=$aip."/manifest-md5.txt";
    my $aipmanifest = $self->swift->object_get($self->container,$manifest);
    if ($aipmanifest->code != 200) {
	croak "object_get container: '".$self->container."' , object: '$manifest'  returned ". $aipmanifest->code . " - " . $aipmanifest->message. "\n";
    };
    $return{'manifest date'}=$aipmanifest->object_meta_header('File-Modified');
    $return{'manifest md5'}=$aipmanifest->etag;
    my @lines= split /\n/,$aipmanifest->content;
    foreach my $line (@lines) {
	if ($line =~ /^\s*([^\s]+)\s+([^\s]+)\s*/) {
	    my ($md5,$file)=($1,$2);
	    if (exists $aipdata{$file}) {
		$return{filesize}+=$aipdata{$file}{'bytes'};
		if ($aipdata{$file}{'hash'} ne $md5) {
		    print "MD5 mismatch: ".Dumper($file,$md5,$aipdata{$file}) if $verbose;
		    $return{validate}=0;
		}
		$aipdata{$file}{'checked'}=1;
	    } else {
		print "File '$file' missing from Swift\n"
		    if $verbose;
		$return{validate}=0;
	    }
	}
    }
    if (scalar(@lines) != scalar(keys %aipdata)) {
	$return{validate}=0;
	if ($verbose) {
	    foreach my $key (keys %aipdata) {
		if (! exists $aipdata{$key}{'checked'}) {
		    print "File '$key' is extra in Swift\n"
		}
	    }
	}
    }

    if($return{validate}) {
	# Update CouchDB...
	$self->tdrepo->update_item_repository($aip, {
	    'verified' => 'now',
	    'filesize' => $return{filesize}
					});
    }
    print Dumper (\%return) if $verbose;
    return \%return;
}

1;
