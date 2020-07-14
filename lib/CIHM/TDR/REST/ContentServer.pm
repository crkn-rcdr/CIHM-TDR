package CIHM::TDR::REST::ContentServer;

use DateTime;
use Crypt::JWT;

use Moose;
with 'Role::REST::Client';
use Types::Standard qw(HashRef Str Int Enum HasMethods);
use Config::General;

# Build our own user agent, which will add the header.
sub _build_user_agent {
    my $self = shift;
    require CIHM::TDR::REST::UserAgent;
    return CIHM::TDR::REST::UserAgent->new( %{ $self->clientattrs } );
}

sub BUILD {
    my $self = shift;
    my $args = shift;

    $self->{LocalTZ} = DateTime::TimeZone->new( name => 'local' );
    $self->{conf} = $args->{conf};

    my $config = new Config::General( -ConfigFile => $self->{conf} );
    my %tdrconfig = $config->getall();

    if ( exists $tdrconfig{content} ) {
        if ( !$self->server && defined $tdrconfig{content}{url} ) {
            $self->server( $tdrconfig{content}{url} );
        }
        $self->{clientattrs}->{c7a_id}     = $tdrconfig{content}{key};
        $self->{clientattrs}->{jwt_secret} = $tdrconfig{content}{password};
    }

    # Passed arguments override what is in config file
    if ( defined $args->{c7a_id} ) {
        $self->{clientattrs}->{c7a_id} = $args->{c7a_id};
    }
    if ( defined $args->{jwt_secret} ) {
        $self->{clientattrs}->{jwt_secret} = $args->{jwt_secret};
    }
    if ( !$self->server ) {
        die
"You need to supply Content Server URL (in config file or command line)";
    }

    # JWT specific
    if ( defined $args->{jwt_algorithm} ) {
        $self->{clientattrs}->{jwt_algorithm} = $args->{jwt_algorithm};
    }
    if ( defined $args->{jwt_payload} ) {
        $self->{clientattrs}->{jwt_payload} = $args->{jwt_payload};
    }
}

sub get_clientattrs {
    my ($self) = shift;

    return $self->{clientattrs};
}

1;
