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
	my %swiftopt = (
	    furl_options => { timeout => 120 }
	    );
	foreach ("server","user","password","account", "furl_options") {
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


sub replicateaip {
    my ($self,$aip,$options) = @_;
    
    my $verbose = exists $options->{'verbose'};
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

	# To support AIP updates, check what files already exist
	my %containeropt = (
	    "prefix" => $aip."/"
	    );
	my %aipdata;

	# Need to loop possibly multiple times as Swift has a maximum of
	# 10,000 names.
	my $more=1;
	while ($more) {
	    my $aipdataresp = $self->swift->container_get($self->container,
							  \%containeropt);
	    if ($aipdataresp->code != 200) {
		croak "container_get(".$self->container.") for $aip returned ". $aipdataresp->code . " - " . $aipdataresp->message. "\n";
	    };
	    $more=scalar(@{$aipdataresp->content});
	    if ($more) {
		$containeropt{'marker'}=$aipdataresp->content->[$more-1]->{name};

		foreach my $object (@{$aipdataresp->content}) {
		    my $file=substr $object->{name},(length $aip)+1;
		    $aipdata{$file}=$object;
		}
	    }
	    undef $aipdataresp;
	}

	{
	    # Load manifest to get MD5 of data files.
	    my $aipfile = $aippath."/manifest-md5.txt";
	    open(my $fh, '<:raw', $aipfile)
		or die "replicate_aip: Could not open file '$aipfile' $!\n";
	    chomp(my @lines = <$fh>);
	    close $fh;
	    foreach my $line (@lines) {
		if ($line =~ /^\s*([^\s]+)\s+([^\s]+)\s*/) {
		    my ($md5,$file)=($1,$2);
		    if (exists $aipdata{$file}) {
			# Fill in md5 from manifest to compare before sending
			$aipdata{$file}{'md5'} = $md5;
		    }
		}
	    }

	    # looping through filenames found on filesystem.
	    foreach my $aipfile (@aipfiles) {
		my $file = substr $aipfile, (length $aippath)+1;
		my $object = $aip . '/' . $file;

		# Check if file with same md5 already on Swift
		if (! exists $aipdata{$file} ||
		    ! exists $aipdata{$file}{'md5'} ||
		    $aipdata{$file}{'md5'} ne  $aipdata{$file}{'hash'}
		    ) {

		    open(my $fh, '<:raw', $aipfile)
			or die "replicate_aip: Could not open file '$aipfile' $!\n";

		    my $filedate="unknown";
		    my $mtime=(stat($fh))[9];
		    if ($mtime) {
			my $dt = DateTime->from_epoch(epoch => $mtime);
			$filedate = $dt->datetime. "Z";
		    }
		    print "Put $object\n" if $verbose;

		    my $putresp = $self->swift->object_put($self->container,$object, $fh, { 'File-Modified' => $filedate});
		    if ($putresp->code != 201) {
			die("object_put of $object returned ".$putresp->code . " - " . $putresp->message."\n");
		    }
		    close $fh;
		} elsif ($verbose) {
		    #print $object." already exists on Swift\n";
		}
		# Remove key, to allow detection of extra files in Swift
		delete  $aipdata{$file};
	    }
	    if (keys %aipdata) {
		# These files existed on Swift, but not on disk, so delete
		# (Files with different names in different AIP revision)
		foreach my $key (keys %aipdata) {
		    my $delresp =
			$self->swift->object_delete($self->container,
						    $aipdata{$key}{'name'});
		    if ($delresp->code != 204) {
			$self->log->warn("object_delete of ". $aipdata{$key}{'name'}." returned ".$delresp->code . " - " . $delresp->message);
		    }
		}
	    }
	}
	my $validate = $self->validateaip($aip,$options);
	if ($validate->{'validate'}) {
	    if ($updatedoc->{'manifest date'} ne $validate->{'manifest date'}) {
		carp "Manifest Date Mismatch: ".$updatedoc->{'manifest date'}." != ".$validate->{'manifest date'}."\n"; # This shouldn't ever happen
	    }
	    if ($updatedoc->{'manifest md5'} ne $validate->{'manifest md5'}) {
		carp "Manifest MD5 Mismatch: ".$updatedoc->{'manifest md5'}." != ".$validate->{'manifest md5'}."\n"; # This shouldn't ever happen
	    }
	} else {
	    $self->log->warn("validation of $aip failed");
	}
    } else {
	carp "$aip not found\n";  # This shouldn't ever happen
    }
    
    # Inform database
    $self->tdrepo->update_item_repository($aip,$updatedoc);
}

# Options include: 'skip:i','timelimit:i','limit:i','verbose','aip:s'
sub validate {
    my ($self,$options) = @_;

    if (exists $options->{aip})  {
	$self->validateaip($options->{aip},$options);
    } else {

	my $start_time=time();
	my $validatecount=0;
	my $errorcount=0;

	$self->log->info("Running validation at: $start_time");
	my $repotxt="reduce=false&startkey=[\"swift\"]&endkey=[\"swift\",{}]";
	if ($options->{limit}) {
	    $repotxt .= "&limit=".$options->{limit};
	}
	if ($options->{skip}) {
	    $repotxt .= "&skip=".$options->{skip};
	}

	my $res = $self->tdrepo->get("/".$self->tdrepo->database."/_design/tdr/_view/repopoolverified?$repotxt",{}, {deserializer => 'application/json'});

	if ($res->code != 200) {
	    croak "repopoolverified for validate returned ". $res->code . " - " . $res->error. "\n";
	};
	my @aiplist;
	foreach my $aipdoc (@{$res->data->{rows}}) {
	    push @aiplist, $aipdoc->{value};
	};
	undef $res;

	foreach my $aip (@aiplist) {
	    if (exists $options->{timelimit} &&
		(time()-$start_time)> $options->{timelimit}) {
		last;
	    };
	    my $val = $self->validateaip($aip,$options);
	    if ($val->{validate}) {
		$validatecount++;
		$self->log->info("verified Swift AIP: $aip");
	    } else {
		$errorcount++;
		$self->log->warn("invalid Swift AIP: $aip");
		print "invalid Swift AIP: $aip\n";
	    }
	}
	print "total valid bags: $validatecount\n";
	print "total invalid bags: $errorcount\n";
	$self->log->info("total valid bags: $validatecount invalid: $errorcount");
	print "total time: ".(time()-$start_time)."\n";
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

    my %containeropt = (
	"prefix" => $aip."/data/"
	);
    my %aipdata;

    # Need to loop possibly multiple times as Swift has a maximum of
    # 10,000 names.
    my $more=1;
    while ($more) {
        my $aipdataresp = $self->swift->container_get($self->container, \%containeropt);
	if ($aipdataresp->code != 200) {
	    croak "container_get(".$self->container.") for $aip/data/ for validate_aip returned ". $aipdataresp->code . " - " . $aipdataresp->message. "\n";
	};
	$more=scalar(@{$aipdataresp->content});
	if ($more) {
	    $containeropt{'marker'}=$aipdataresp->content->[$more-1]->{name};

	    foreach my $object (@{$aipdataresp->content}) {
		my $file=substr $object->{name},(length $aip)+1;
		$aipdata{$file}=$object;
	    }
	}
	undef $aipdataresp;
    }
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

sub walk {
    my ($self,$options) = @_;


    # Get list from CouchDB
    my $res = $self->tdrepo->get("/".$self->tdrepo->database."/_design/tdr/_list/manifestinfo/tdr/repoown?reduce=false&startkey=[\"swift\"]&endkey=[\"swift\",{}]&include_docs=true",{}, {});
    if ($res->code != 200) {
	print STDERR ("Walk tdrepo->get return code: ".$res->code . "\nError: " . $res->error ."\n");
    }
    if ($res->failed) {
	die ("Walk tdrepo->get failed flag set\n". $res->response->as_string() . "\n");
    }
    if (! keys $res->data) {
	die ("Walk tdrepo->get empty hash\n". $res->response->as_string() . "\n");
    }
    my $aiplist=$res->data;

    # Count of AIPs with each storage
    my %counts = (
	swift => 0,
	couch => scalar (keys %{$aiplist})
	);

    # Walk though AIP list in Swift
    my %containeropt = (
	 delimiter => "/"
	);
    # Need to loop possibly multiple times as Swift has a maximum of
    # 10,000 names.
    my $more=1;
    while ($more) {
	my $aipdataresp = $self->swift->container_get($self->container,
							  \%containeropt);
	if ($aipdataresp->code != 200) {
	    croak "container_get(".$self->container.") returned ". $aipdataresp->code . " - " . $aipdataresp->message. "\n";
	};
	$more=scalar(@{$aipdataresp->content});
	if ($more) {
	    $containeropt{'marker'}=$aipdataresp->content->[$more-1]->{subdir};

	    foreach my $object (@{$aipdataresp->content}) {
		my $aip = substr $object->{subdir},0,-1;
		$self->walk_aip($options,$aip,$aiplist, \%counts);
	    }
	}
    }

    my $aiplistcount = keys %{$aiplist};
    if (! $options->{quiet} && ($aiplistcount > 0)) {
	print "There were $aiplistcount AIPs only found in DB:\n--begin--\n";
	foreach my $aipkey (keys %{$aiplist}) {
	    print "$aipkey\n";
	}
	print "--end--\n";
    }

    print $counts{swift} . " AIPS found in Swift";
    if ($counts{swift} == $counts{couch}) {
	print .".\n";
    } else {
	print ", only ".$counts{couch}." in database.\n";
    }
}

sub walk_aip {
    my ($self,$options,$aip,$aiplist,$counts) = @_;

    my $count = ($counts->{swift})++;

    my $aipres = $self->swift->object_head($self->container,
					   "$aip/manifest-md5.txt");
    if ($aipres->code != 200) {
	die "object_head: '".$self->container."' , object: 'aip/manifest-md5.txt'  returned ". $aipres->code . " - " . $aipres->message. "\n";
    };


    my $update=0;
    my $updatedoc = {
	'manifest md5' => $aipres->etag,
	'manifest date' => $aipres->object_meta_header('file-modified')
    };

    if (exists $aiplist->{$aip} && exists $aiplist->{$aip}->{'manifest md5'}) {
	if ( $aiplist->{$aip}->{'manifest md5'} ne
	     $updatedoc->{'manifest md5'}) {
	    $update=1;
	    if (! $options->{quiet}) {
		print "MD5 mismatch $aip: ".$self->{aiplist}->{$aip}->{'manifest md5'}." != ".$updatedoc->{'manifest md5'}."\n";
	    }
	}

	if (! exists $aiplist->{$aip}->{'manifest date'}) {
	    # Initialize variable -- will be noticed as mismatch, but without PERL error
	    $aiplist->{$aip}->{'manifest date'}='[unset]';
	}
	if ( $aiplist->{$aip}->{'manifest date'} ne
	     $updatedoc->{'manifest date'}) {
	    $update=1;
	    if (! $options->{quiet}) {
		print "Date mismatch $aip: ".$aiplist->{$aip}->{'manifest date'}." != ".$updatedoc->{'manifest date'}."\n";
	    }
	}
	delete $aiplist->{$aip};

    } else {

	if (! $options->{quiet}) {
	    print "New Swift AIP found: $aip\n";
	}
	$update=1;
    }

    if ($update && $options->{update}) {
	if (! $options->{quietupdate}) {
	    print "Updating: " .
		Data::Dumper->new([$updatedoc],[$aip])->Dump . "\n";
	}
	$self->tdr_repo->update_item_repository($aip,$updatedoc);
    }
}

1;
