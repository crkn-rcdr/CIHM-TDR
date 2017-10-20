package CIHM::TDR::Message;

use base qw(Class::Singleton);
use CIHM::TDR::TDRConfig;
use AnyEvent::Beanstalk;

sub _new_instance {
  my $class = shift;
  my $t_config = shift;
  my $self  = bless {} , $class;
  $self->{t_config}  = $t_config;
  $self->{logger}    = $self->{t_config}->{logger};
  $self->{bs_client} = AnyEvent::Beanstalk->new (
        server=>"localhost",
      );

  return $self;
}

sub send {
  my $self  = shift;
  my $level = shift;
  my $msg   = shift;

  $self->{bs_client}->use("Logger")->recv;

  my $data={  level=> $level,
              msg  => $msg,}; 

  $self->{bs_client}->put(
    { encode => $data }, sub {

  });
  

}

sub recv {
  my $self = shift;
  my $level = shift;
  my $client = $self->{bs_client};
  my $logger = $self->{logger};
  my $cv = AE::cv;
  $client->reserve( sub {
          my $job = shift;
          $logger->debug("got job: ".Dumper($job));
          my $args = {};
          my $decode = $job->decode;
          $args->{level} = $decode->{level};
          $args->{msg}   = $decode->{msg};
          $args->{job_id} = $job->id;
          });
  my $job_id = $cv->recv;
  my $elapsed = tv_interval ( $t0, [gettimeofday]);

  $logger->info("success: time: $elapsed ".$job_id);
  $client->delete($job_id);

}
1;
