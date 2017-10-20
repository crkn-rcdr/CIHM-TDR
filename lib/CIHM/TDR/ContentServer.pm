package CIHM::TDR::ContentServer;

use strict;
use Config::General;
use Data::Dumper;
use CIHM::TDR::TDRConfig;
use CIHM::TDR::Repository;
use CIHM::TDR::REST::ContentServer;

=head1 NAME

CIHM::TDR::ContentServer - TDR Content Server access

=head1 SYNOPSIS

    my $t_repo = CIHM::TDR::ContentServer->new($configpath);
      where $configpath is as defined in CIHM::TDR::TDRConfig

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

    # Confirm there is a ContentServer block in the config
    my %confighash = %{$self->{config}->get_conf};
    if (! (%confighash && $confighash{'contentserver'})) {
        return;
    }
    $self->{serverconf} = $confighash{'contentserver'};
    return $self;
}

sub config_path {
    my $self = shift;
    return $self->{config_path};
}

# Return the hash of servers
sub conf_servers {
    my $self = shift;

    my %servers;
    if(defined $self->{serverconf}{'server'}) {
        %servers = %{$self->{serverconf}{'server'}};
    }
    return (%servers);
}

# Return an array listing all the servers
sub servers {
    my $self = shift;
    my %servers = $self->conf_servers;
    return (keys %servers);
}

# Return hashref for specific repository
sub conf_server_repository {
    my ($self,$repository) = @_;

    my %servers = $self->conf_servers;
    while ( my ($coskey, $cosconf) = each %servers ) {
        if ($cosconf->{repository} eq $repository) {
            return $cosconf;
        }
    }
    return;
}


# Returns a sorted (by replication priority) array of repository names
# (Note: repositories, not server names which often don't match)
sub replication_repositories {
    my $self = shift;
    my %servers = $self->conf_servers;

    # Just return it if we've already calculated
    if ($self->{repriority}) {
        return @{$self->{repriority}};
    }

    # Create ascii sortable array using replication priority and repository
    my @repriority = ();
    foreach my $server (keys %servers) {
        my %myserver = %{$servers{$server}};
        if (%myserver && $myserver{'replication'} && $myserver{'repository'}) {
            push(@repriority,sprintf("%02d:%s",$myserver{'replication'},$myserver{'repository'}));
        }
    }

    # Sort the array by priority, and then split out only the repository names
    my @repri = ();
    foreach my $reprio (sort @repriority) {
        my ($priority,$repository) = split(":",$reprio);
        push (@repri,$repository);
    }
    $self->{repriority}=\@repri;
    return(@repri);
}


# TODO: sub to return server given server name or undef for default
#
#    if (!$repository) {
#        $repository=$self->{serverconf}{'default'};
#        if (!$repository) {
#            $repository=shift keys %servers;
#        }
#    }

# Returns configuration block for server matching repository
sub find_server_repository {
    my $self = shift;
    my $repository = shift;

    my %servers = $self->conf_servers;
    foreach my $server (keys %servers) {
        my %myserver = %{$servers{$server}};
        if ($myserver{repository} eq $repository) {
            return \%myserver;
        }
    }
    return;
}

# Returns a ContentServer object for server matching repository
sub new_RepositoryContentServer {
    my $self = shift;
    my $repository = shift;

    my $repoconf=$self->find_server_repository($repository);
    if (!$repoconf) {
        return;
    }
    my %cosargs = (
        c7a_id => $repoconf->{'key'},
        c7a_secret => $repoconf->{'password'},
        server => $repoconf->{'url'},
        );
    return new CIHM::TDR::REST::ContentServer (\%cosargs);
}

sub repository {
    my $self = shift;

    if (!($self->{repository})) {
        $self->{repository}= new CIHM::TDR::Repository($self->config_path);
    }
    return $self->{repository};
}

# TODO: Some services such as Export and Ingest don't need to be run
# on repositories.
sub tdrepo {
    my $self = shift;

    if (! ($self->repository->tdrepo)) {
        die "No tdrepo access set up\n";
    }
    return $self->repository->tdrepo;
}

sub get_aipinfo {
    my ($self,$aip,$repository) = @_;

    my $aipinfo=$self->tdrepo->get_item_otherrepo($aip,$repository);
    if (! $aipinfo) {
        return {};
    }
    my $confrepo=$self->conf_server_repository($repository);
    if ($confrepo && defined $confrepo->{rsyncpath} 
        && defined $aipinfo->{pool}) {
        $aipinfo->{'rsyncpath'}=join('/',$confrepo->{rsyncpath},
                                     $aipinfo->{pool},
                                     $self->repository->path_uid($aip));
    }
    return $aipinfo;
}



1;
