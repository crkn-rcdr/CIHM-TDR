package CIHM::TDR::App::UpdateMetadata;

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

parameter 'file' => (
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
    documentation => q[Filename of the metadata .xml file],
);

parameter 'reason' => (
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
    documentation => q[Text for changelog describing reason for the update],
);

command_short_description 'Updates the metadata for an existing AIP';

sub run {
    my ($self) = @_;

    my $TDR    = CIHM::TDR->new( $self->conf );
    my $t_repo = $TDR->{repo};

    my $uid;

    # Convert UID on command line to simple UID (may be path)
    if ( !( $uid = ( $t_repo->path_uid( $self->uid ) )[2] ) ) {
        die "'" . $self->uid . "' is not a UID\n";
    }
    my $metadata = $self->file;
    if ( !-f $metadata ) {
        print STDERR "Metadata file not found: $metadata\n";
        return;
    }
    my ( $contributor, $identifier ) = split( /\./, $uid );

    # Get the md5 of the metadata.xml to compare with all previous revisions.
    open( METADATA, "<", $self->file )
      or die "Can't open metadata.xml at" . $self->file . "\n";
    binmode(METADATA);
    my $metadatamd5 = Digest::MD5->new->addfile(*METADATA)->hexdigest;
    close(METADATA);

    # Checks to determine if the environment is set correctly for ingest.
    $TDR->ingest_check( $contributor, $identifier, 1, $metadatamd5 );

    eval {
        $TDR->update_metadata(
            $contributor,  $identifier, $self->file,
            $self->reason, $self->pool
        );
    };
    die("Removal failed: $@\n") if ($@);
}
1;
