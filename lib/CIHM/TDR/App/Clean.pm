package CIHM::TDR::App::Clean;

use common::sense;
use Data::Dumper;
use MooseX::App::Command;
use File::Path qw(make_path remove_tree);

extends qw(CIHM::TDR::App);

parameter 'uid' => (
    is  => 'rw',
    isa => 'Str',
    documentation =>
      q[The uid of the AIP (In contributor.identifier or identifier form)],
);

option 'contributor' => (
    is            => 'rw',
    isa           => 'Str',
    default       => 'oocihm',
    documentation => q[Contributor to use if not supplied with uid],
);

option 'emptytrash' => (
    is            => 'rw',
    isa           => 'Bool',
    documentation => q[Cleans out all aips in trashcan older than age given],
);

option 'trashage' => (
    is            => 'rw',
    isa           => 'Int',
    documentation => q[Age for removing trash (in hours)],
);

command_short_description 'Cleans up incoming and trash';
command_usage 'tdr clean [<uid> ...] [long options...]';

sub run {
    my ($self) = @_;

    use CIHM::TDR;
    use CIHM::TDR::Repository;
    use Archive::BagIt;

    my $TDR    = CIHM::TDR->new( $self->conf );
    my $t_repo = $TDR->{repo};

    my @uids = @{ ( $self->extra_argv )[0] };
    unshift( @uids, $self->uid );

    foreach my $param_uid (@uids) {
        if ( index( $param_uid, "." ) == -1 ) {
            $param_uid = $self->contributor . "." . $param_uid;
        }

        my $uid;

        # Convert UID on command line to simple UID (may be path)
        if ( !( $uid = ( $t_repo->path_uid($param_uid) )[2] ) ) {
            die "'$param_uid' is not a UID\n";
        }
        my ( $contributor, $identifier ) = split( /\./, $uid );
        $TDR->clean( $contributor, $identifier );
    }
}

1;
