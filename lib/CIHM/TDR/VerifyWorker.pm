package CIHM::TDR::VerifyWorker;

use strict;
use AnyEvent;
use Try::Tiny;
use Storable qw(thaw freeze);

sub bag_verify {
    my ($bag_path) = @_;
    use Archive::BagIt::Fast;
    AE::log debug => "checking bag: $bag_path";
    my $bagit;
    my $valid;
    my $try_success = try {
        $bagit = new Archive::BagIt::Fast($bag_path);
        $valid = $bagit->verify_bag( { mmap_min => 8000000 } );
        AE::log debug => "valid: $bag_path\n";
        return $valid;
    }
    catch {
        AE::log debug => "caught an invalid $bag_path: $!";
        return undef;
    };

    if ($try_success) {
        return ( "ok", $bag_path, freeze( $bagit->{stats} ) );
    }

    return ( "invalid", $bag_path );

}

1;
