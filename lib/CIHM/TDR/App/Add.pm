package CIHM::TDR::App::Add;

use common::sense;
use Data::Dumper;
use Storable qw(freeze thaw);
use MooseX::App::Command;
use Try::Tiny;
use CIHM::TDR::Repository;
use CIHM::TDR::ContentServer;
use Archive::BagIt::Fast;
use Archive::BagIt;

extends qw(CIHM::TDR::App);

option 'replace' => (
    is            => 'rw',
    isa           => 'Bool',
    documentation => q[Yes I mean replace ],
);

option 'noverify' => (
    is            => 'rw',
    isa           => 'Bool',
    documentation => q[Skip verification. I've done it already],
);

option 'rsync' => (
    is            => 'rw',
    isa           => 'Bool',
    documentation => q[uid is an AIP directory to rsync into incoming and add],
);

option 'cosrsync' => (
    is  => 'rw',
    isa => 'Bool',
    documentation =>
q[uid is a UID to query against a COS to then rsync into incoming and add],
);

option 'pool' => (
    is            => 'rw',
    isa           => 'Str',
    documentation => q[Name of the pool to add the AIP to],
);

parameter 'uid' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
    documentation =>
q[The uid of the AIP (In contributor.identifier form) expected in incoming],
);

option 'repository' => (
    is            => 'rw',
    isa           => 'Str',
    documentation => q[Repository to copy from (Used with --cosrsync)],
);

command_short_description 'adds a bag from incoming';

sub run {
    my ($self) = @_;

    # Fill in some details that will go to CouchDB
    my $updatedoc = {};

    my $t_repo  = CIHM::TDR::Repository->new( $self->conf );
    my $cserver = CIHM::TDR::ContentServer->new( $self->conf );

    my $uid;

    # Convert UID on command line to simple UID (may be path)
    if ( !( $uid = ( $t_repo->path_uid( $self->uid ) )[2] ) ) {
        die "'" . $self->uid . "' is not a UID\n";
    }
    my ( $contributor, $identifier ) = split( /\./, $uid );

    my $aip_path = $t_repo->find_aip_pool( $contributor, $identifier );

    if ($aip_path) {
        $updatedoc = $t_repo->get_manifestinfo($aip_path);

        # The AIP is already here... Make sure it is in the database
        $t_repo->aip_add_db( $contributor, $identifier, $updatedoc );
    }

    my $trashpath = $t_repo->find_trash_pool( $contributor, $identifier );
    if ( $trashpath && $aip_path ) {
        say "$uid already in both trashcan and TDR: $trashpath, $aip_path";
        exit;
    }

    # If there was a command line option, use that
    my $pool = $self->pool;

    # If AIP already has incoming directory, use that
    my @incomingpool = $t_repo->find_incoming_pool( $contributor, $identifier );
    if (@incomingpool) {
        $pool = $incomingpool[0];
    }

    # Otherwise use the pool with the most free space
    if ( !$pool ) {
        $pool = $t_repo->pool_free;
    }
    if ( !$pool ) {
        say "No pool able to be chosen";
        exit;
    }

    if ( $self->cosrsync ) {
        my $repository;
        my $aipinfo;
        if ( $self->repository ) {
            $repository = $self->repository;
            $aipinfo = $cserver->get_aipinfo( $uid, $repository );
        }
        else {
            my @rrepos = $cserver->replication_repositories();
            foreach my $repo (@rrepos) {
                $aipinfo = $cserver->get_aipinfo( $uid, $repo );
                if ( exists $aipinfo->{rsyncpath} ) {
                    last;
                }
            }
        }
        if ( !$aipinfo ) {
            die "Couldn't get AIP information for $repository\n";
        }
        my $rsyncpath = $aipinfo->{rsyncpath};
        if ( !$rsyncpath ) {
            die "Couldn't get rsync information for $repository\n";
        }

        if (  !$self->replace
            && $aipinfo->{'manifest md5'}
            && $updatedoc->{'manifest md5'}
            && $aipinfo->{'manifest md5'} eq $updatedoc->{'manifest md5'} )
        {
            print STDERR "$uid with md5("
              . $updatedoc->{'manifest md5'}
              . ") already exists.\n";
            return;
        }
        $self->uid($rsyncpath);
        $self->rsync(1);
        say "Will rsync from " . $self->uid;
    }

    if ( $self->rsync ) {
        $t_repo->incoming_rsync( $pool, $uid, $self->uid );
    }

    my $incomingpath = $t_repo->find_incoming_pool( $contributor, $identifier );
    if ( !$incomingpath ) {
        say "$uid not found in any incoming path";
        exit;
    }

    if ( !$self->noverify ) {
        try {
            my $bagit = new Archive::BagIt::Fast($incomingpath);
            my $valid = $bagit->verify_bag();

            # If we have the size, then set it in database
            if ( $bagit->{stats} && $bagit->{stats}->{size} ) {
                $updatedoc->{'filesize'} = $bagit->{stats}->{size};
            }
        }
        catch {
            die "invalid source bag: $incomingpath";
        };
    }
    say "adding " . $incomingpath;
    if ($aip_path) {

        # AIP already exists in the TDR
        if ( !$self->replace ) {
            my $src_bagit = new Archive::BagIt($incomingpath);
            my $src_md5   = $src_bagit->get_checksum();
            if ( $src_md5 eq $updatedoc->{'manifest md5'} ) {
                say "Already exists with same checksum";
                exit;
            }
        }
        if ( !( $t_repo->aip_delete( $contributor, $identifier ) ) ) {
            say "Failed to remove AIP";
            exit;
        }
    }
    if ( !( $t_repo->aip_add( $contributor, $identifier, $updatedoc ) ) ) {
        say "Failed to add AIP";
        exit;
    }
}

1;
