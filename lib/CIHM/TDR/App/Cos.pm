package CIHM::TDR::App::Cos;

use common::sense;
use Data::Dumper;
use CIHM::TDR::REST::ContentServer;
use Switch;
use MooseX::App::Command;
extends qw(CIHM::TDR::App);

=head1 NAME

CIHM::TDR::App::Cos - Command-line access to data in a ContentServer

=head1 USAGE

 tdr cos copy <source> <destination> [long options...]
 tdr cos --help

=head1 OVERVIEW

Where:

<source> The source file on the ContentServer, starting with the UID and continuing with the path within the UID of the specific file.

<destination> The filename on the local filesystem where you wish to store the file.  The special name "-" (minus, without quotes) outputs the contents to standard output.

<uid> the UID of the AIP you wish information for.

Examples:

tdr cos copy oocihm.8_04385_37/data/sip/data/metadata.xml metadata.xml

tdr cos copy oocihm.8_04385_37/manifest-md5.txt - | sed -e 's/[^ ]*[ ]*//'

=cut

parameter 'cmd' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[Sub-command is one of: info, copy, printjwt],
);

option 'server' => (
    is => 'rw',
    isa => 'Str',
    documentation => q[Content server to communicate with],
);

option 'key' => (
    is => 'rw',
    isa => 'Str',
    documentation => q[Key ID to use with Content server],
);

option 'password' => (
    is => 'rw',
    isa => 'Str',
    documentation => q[Password to use with Content server],
);

option 'algorithm' => (
    is => 'rw',
    isa => 'Str',
    default => 'HS256',
    documentation => q[Algorithm used for signing the JWT],
);

option 'payload' => (
    is => 'rw',
    isa => 'Str',
    default => '{"uids":[".*"]}',
    documentation => q[JWT payload],
);

command_short_description 'Requests data from a content server';

sub run {
    my ($self) = @_;

    my %cosargs = (
        conf => $self->conf,
        c7a_id => $self->key,
        jwt_secret => $self->password,
        jwt_algorithm => $self->algorithm,
        jwt_payload => $self->payload
        );
    # Only provide this argument if it exists...
    if ($self->server) {
        $cosargs{server}=$self->server;
    }
    my $COS_REST = new CIHM::TDR::REST::ContentServer (\%cosargs);


    switch ($self->cmd) {
        case "copy" {
            my $file = shift $self->extra_argv;
            if (!$file) {
                say STDERR "Missing source for copy";
                exit 1;
            }
            my $tofile = shift $self->extra_argv;
            if (!$tofile) {
                say STDERR "Missing destingation filename for copy";
                exit 1;
            }
            my $r = $COS_REST->get("/$file", {file => $file});
            if ($r->code == 200) {
                my $fh;
                if ($tofile eq "-") {
                    open ($fh,">-");
                } else {
                    open($fh, '>', $tofile)
                        or die "cannot open $tofile : $!";
                }
                print $fh $r->response->content;
                close;
            } elsif ($r->code == 599) {
                say $r->response->content;
            } else {
                say "Received return code: " . $r->code;
#                say "Reponse: " . Dumper ($r->response);
            }
        }
        case "printjwt" {
            use Crypt::JWT qw(encode_jwt);

            my $clientattrs=  $COS_REST->get_clientattrs;

            my $jws_token = encode_jwt(payload=>$clientattrs->{jwt_payload}, 
                                       alg=>$clientattrs->{jwt_algorithm},
                                       key=>$clientattrs->{jwt_secret});

            say "Payload=".$clientattrs->{jwt_payload};
            say "Token=$jws_token";
        }
    }
}

1;
