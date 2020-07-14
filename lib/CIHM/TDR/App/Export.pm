package CIHM::TDR::App::Export;

use common::sense;
use Data::Dumper;
use MooseX::App::Command;
use Switch;
use File::Copy;

extends qw(CIHM::TDR::App);

parameter 'cmd' => (
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
    documentation => q[Sub-command is one of: sip, revision, metadata, dmd],
);

parameter 'uid' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
    documentation =>
      q[The uid of the AIP (In contributor.identifier or identifier form)],
);

option 'contributor' => (
    is            => 'rw',
    isa           => 'Str',
    default       => 'oocihm',
    documentation => q[Contributor to use if not supplied with uid],
);

option 'revision' => (
    is            => 'rw',
    isa           => 'Str',
    documentation => q[Revision (for export revision)],
);

command_short_description 'Exports information from the repository';
command_usage
  'tdr export <sip|revision|metadata|dmd> [<uid> ...] [long options...]';

sub run {
    my ($self) = @_;

    use CIHM::TDR;
    use CIHM::TDR::Repository;
    use Archive::BagIt;

    my $TDR    = CIHM::TDR->new( $self->conf );
    my $t_repo = $TDR->{repo};

    if ( !( ( $self->cmd ) =~ /(sip|revision|metadata|dmd)/ ) ) {
        print "Invalid sub-command: " . $self->cmd . "\n";
        return;
    }

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
        my $aip = $t_repo->find_aip_pool( $contributor, $identifier );
        if ($aip) {
            switch ( $self->cmd ) {
                case "sip" {
                    my $target = "$contributor.$identifier";
                    eval {
                        $TDR->export_sip( $contributor, $identifier, $target );
                    };
                    if ($@) {
                        warn("Failed to export $identifier: $@\n");
                    }
                }
                case "revision" {
                    my $revision = $self->revision;
                    if ($revision) {
                        my $target = "$contributor.$identifier.$revision";
                        eval {
                            $TDR->export_revision(
                                $contributor, $identifier,
                                $revision,    $target
                            );
                        };
                        if ($@) {
                            warn("Failed to export $identifier: $@\n");
                        }
                    }
                    else {
                        warn("Missing --revision\n");
                        last;
                    }
                }
                case "metadata" {
                    my $target   = "$contributor.$identifier.xml";
                    my $metadata = "$aip/data/sip/data/metadata.xml";
                    if ( !-f $metadata ) {
                        die("Cannot fine $metadata in $aip");
                    }
                    copy( $metadata, $target )
                      or die("Failed to export $metadata to $target: $!");
                }
                case "dmd" {
                    my $target = "$contributor.$identifier.xml";
                    eval {
                        $TDR->export_dmd( $contributor, $identifier, $target );
                    };
                    if ($@) {
                        warn("Failed to export $identifier: $@\n");
                    }
                }

            }
        }
        else {
            print "$contributor.$identifier [MISSING]\n";
        }
    }
}

1;
