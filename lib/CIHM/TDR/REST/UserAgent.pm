package CIHM::TDR::REST::UserAgent;

use 5.006;
use strict;
use warnings FATAL => 'all';

use parent qw(HTTP::Thin);
use URI;
use URI::Escape;
use DateTime;
use Crypt::JWT qw(encode_jwt);
use JSON;

our $VERSION = '0.03';

sub new {
	my ($class, %args) = @_;

	my $self = $class->SUPER::new(%args);

	$self->{c7a_id} = $args{c7a_id} || '';
	$self->{jwt_secret} = $args{jwt_secret} || '';
        $self->{jwt_algorithm} = $args{jwt_algorithm} || 'HS256';
        my $payload = $args{jwt_payload} || {};

        if(ref($payload) =~ /^(HASH|ARRAY)$/) {
            $self->{jwt_payload}=$payload;
        } else {
            $self->{jwt_payload}=decode_json($payload);
        }
        # Set iss
        $self->{jwt_payload}->{iss}=$self->{c7a_id};

	return bless $self, $class;
}

# override this HTTP::Tiny method to add authorization/date headers
sub _prepare_headers_and_cb {
	my ($self, $request, $args, $url, $auth) = @_;
	$self->SUPER::_prepare_headers_and_cb($request, $args, $url, $auth);

	# add our own very special authorization headers
	if ($self->{c7a_id} && $self->{jwt_secret}) {
		$self->_add_c7a_headers($request, $args);
	}

	return;
}


sub encode_param {
    my $param = shift;
    URI::Escape::uri_escape($param, '^\w.~-');
}

sub _add_c7a_headers {
	my ($self, $request, $args) = @_;
	my $uri = URI->new($request->{uri});
	my $method = uc $request->{method};

        my $jws_token = encode_jwt(payload=>$self->{jwt_payload}, 
                                   alg=>$self->{jwt_algorithm},
                                   key=>$self->{jwt_secret});

	$request->{headers}{'Authorization'} = 
            "C7A2 ".encode_param($jws_token);
	return;
}

1;
