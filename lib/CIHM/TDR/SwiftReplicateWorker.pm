package CIHM::TDR::SwiftReplicateWorker;

use strict;
use Carp;
use AnyEvent;
use Try::Tiny;

use CIHM::TDR::Swift;

our $self;

sub initworker {
    my $configpath = shift;
    our $self;

    $self = bless {};
    $self->{swift} = CIHM::TDR::Swift->new(
        {
            configpath => $configpath
        }
    );

}

sub swift {
    my $self = shift;
    return $self->{swift};
}

sub aip {
    my $self = shift;
    my $aip  = $self->{aip};
    if ( !$aip ) {
        $aip = "unknown";
    }
    return $aip;
}

sub log {
    my $self = shift;
    return $self->swift->log;
}

sub warnings {
    my $warning = shift;
    our $self;

    # Strip wide characters before  trying to log
    ( my $stripped = $warning ) =~ s/[^\x00-\x7f]//g;

    if ($self) {
        print STDERR "$warning\n";
        $self->log->warn( $self->aip . ": $stripped" );
    }
    else {
        say STDERR "$warning\n";
    }
}

sub replicateaip {
    my ( $aip, $configpath, $verbose, $fromswift ) = @_;
    our $self;

    # Capture warnings
    local $SIG{__WARN__} = sub { &warnings };

    if ( !$self ) {
        initworker($configpath);
    }

    $self->{aip} = $aip;
    my $error;
    try {
        my %options;
        if ($verbose) {
            $options{'verbose'} = 1;
        }
        if ($fromswift) {
            $self->swift->replicateaipfrom( $aip, \%options );
        }
        else {
            $self->swift->replicateaip( $aip, \%options );
        }
    }
    catch {
        $self->log->error("$aip: $_");
        $error = 1;
    };
    return ( $aip, $error );
}

1;
