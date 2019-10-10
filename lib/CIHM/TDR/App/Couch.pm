package CIHM::TDR::App::Couch;

use common::sense;
use Data::Dumper;
use MooseX::App::Command;
use CIHM::TDR::TDRConfig;
use CIHM::TDR::Repository;
use CIHM::TDR::Replication;
use Try::Tiny;
use Archive::BagIt::Fast;

extends qw(CIHM::TDR::App);

use Log::Log4perl;
with 'MooseX::Log::Log4perl';

BEGIN {
  Log::Log4perl->init_once("/etc/canadiana/tdr/log4perl.conf");
}


parameter 'cmd' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[One of {sync}],
);

option 'since' => (
    is => 'rw',
    isa => 'Str',
    documentation => q[A start datetime (or number of hours ago) to limit how far back to look for new AIPs (diff)],
);

command_short_description 'Interacts with tdrepo database';

sub run {
    my ($self) = @_;

    my $t_rep = new CIHM::TDR::Replication({
        configpath => $self->conf,
        since => $self->since
                                           });

    my $cmd = $self->cmd;   
    if ($cmd eq "diff") {
        my $fromrepo = shift $self->extra_argv;

        my $newestaips = $t_rep->tdrepo->get_newestaip({date => $self->since});
        if (!$newestaips || !scalar(@$newestaips)) {
            print STDERR "Nothing new....\n";
            return;
        }

        # Loop through all the changed AIPs to see if the AIP is on
        # the machine I want to replicate from
        foreach my $thisaip (@$newestaips) {
            my $aip = $thisaip->{key};
            my @repos = @{$thisaip->{value}[1]};
            foreach my $thisrepo (@repos) {
                if ($thisrepo eq $fromrepo) {
                    print $aip."\n";
                    next;
                }
            }
        }
    } elsif ($cmd eq "stats") {
        my $stats =  $t_rep->tdrepo->get_repostats();
        if ($stats) {
            foreach my $thissrvr (@$stats) {
                printf ("%20s | %d\n",$thissrvr->{key}[0], $thissrvr->{value});
            }
        }
    } elsif ($cmd eq "replicationwork") {
	$t_rep->replicationwork();
    } elsif ($cmd eq "replicate") {
	$t_rep->replicate();
    } elsif ($cmd eq "test") {
	my $aip = shift $self->extra_argv;
	print "$aip : \n";

            my $priority = 5;  # Set default priority
            my $match = $t_rep->tdr_repo->aip_match($aip);
            if ($match && $match->{replicate}) {
                $priority = $match->{replicate};
            }

        print "Match: Priority=$priority , " . Dumper($match) . "\n";

        #print Dumper(\%deps);
    } else {
        say "`$cmd` unknown";
    }
}

1;
