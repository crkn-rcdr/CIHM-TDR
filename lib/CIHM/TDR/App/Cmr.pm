package CIHM::TDR::App::Cmr;

use common::sense;
use Data::Dumper;
use Storable qw(freeze thaw);
use MooseX::App::Command;
use Try::Tiny;
use CIHM::TDR;
use Archive::BagIt;

extends qw(CIHM::TDR::App);

option 'pool' => (
    is  => 'rw',
    isa => 'Str',
    documentation =>
q[Name of the pool to store the AIP (Default is pool with most available space)],
);

parameter 'uid' => (
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
    documentation => q[The uid of the AIP (In contributor.identifier form)],
);

command_short_description 'Rebuild an AIP\'s CMR record';

sub run {
    my ($self) = @_;

    my $TDR    = CIHM::TDR->new( $self->conf );
    my $t_repo = $TDR->{repo};

    my $uid;

    # Convert UID on command line to simple UID (may be path)
    if ( !( $uid = ( $t_repo->path_uid( $self->uid ) )[2] ) ) {
        die "'" . $self->uid . "' is not a UID\n";
    }

    my ( $contributor, $identifier ) = split( /\./, $uid );
    eval { $TDR->rebuild_cmr( $contributor, $identifier, $self->pool ) };
    die("CMR build failed: $@\n") if ($@);
}

1;
