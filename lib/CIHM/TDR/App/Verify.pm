package CIHM::TDR::App::Verify;

use common::sense;
use feature "switch";
use Data::Dumper;
use Storable qw(freeze thaw);
use MooseX::App::Command;
use CIHM::TDR::VerifyWorker;
use DateTime;

extends qw(CIHM::TDR::App);

use Log::Log4perl;
with 'MooseX::Log::Log4perl';

BEGIN {
  Log::Log4perl->init_once("/etc/canadiana/tdr/log4perl.conf");
}


option 'limit' => (
  is=>'rw',
  isa => 'Int',
  default => 20,
  documentation => q[when selecting bags from the database set a per-request limit],
);

option 'timelimit' => (
  is=>'rw',
  isa => 'Int',
  default => 86400,
  documentation => q[a second time limit],
);

option 'maxprocs' => (
  is => 'rw',
  isa => 'Int',
  default => 4,
  documentation => q[For areas where multiple processes need to exist],
);

option 'workqueue' => (
  is => 'rw',
  isa => 'Int',
  default => 2,
  documentation => q[This is how you'd adjust the worker queue if you need to ],
);

option 'verbose' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[For extra output],
);

command_short_description 'Verifies bags from the least recently checked in the database';


sub run {
  my ($self) = @_;

  use CIHM::TDR::TDRConfig;
  use CIHM::TDR::Repository;
  use Coro::Semaphore;
  use AnyEvent::Fork::Pool;

  my $t_repo = CIHM::TDR::Repository->new($self->conf);
  $self->{t_repo}=$t_repo;

  if (!$t_repo->tdrepo) {
      say "Missing <tdrepo> configuration block in config\n";
      exit;
  }

  my $verbose = $self->verbose || 0;

  # Basic counters
  my $stats = {};
  $stats->{verified}->{count} = 0;
  $stats->{error}->{count} = 0;


  my $start_time=time();
  $self->log->info("Running tdr verify at: $start_time");
  my $pool = AnyEvent::Fork
      ->new
      ->require ("CIHM::TDR::VerifyWorker")
      ->AnyEvent::Fork::Pool::run (
        "CIHM::TDR::VerifyWorker::bag_verify",
        "max"        => $self->maxprocs,
        "load"       => $self->workqueue,
        "on_destroy" => ( my $cv_finish = AE::cv ),
  );
   
    my $sem = new Coro::Semaphore ($self->maxprocs*$self->workqueue);
    my $time_remaining = 1;
    while(my $uid = $self->next_uid() ) { 
      last if !$time_remaining;
      $sem->down;
      if ((time()-$start_time)>$self->timelimit) { $time_remaining = 0; }

      my ($contributor, $identifier) = split(/\./,$uid);
      my $aippath = $t_repo->find_aip_pool($contributor,$identifier);
      if (!$aippath) {
          print STDERR "Couldn't find: $contributor.$identifier\n";
          next;
      }
      print "verifying $aippath\n" if $verbose;

      my $bag_start_time = time();
      $pool->($aippath, sub {
        my $ver_res = shift;
        my $ver_path = shift;
        my $bag_stats   = shift;
        $sem->up;
        if($ver_res eq "ok") {
          print "bag_stats:" if $verbose;
          $bag_stats = thaw($bag_stats);

          # Update CouchDB...
          $t_repo->update_item_repository($uid, {
              'verified' => 'now',
              'filesize' => $bag_stats->{size}
                                          });

          my $bag_end_time = time();
          $stats->{verified}->{count}++;
          print "verified $ver_path ($uid) in ".($bag_end_time-$bag_start_time)."s\n" if $verbose;
          print "verify_time: ".$bag_stats->{verify_time}."\n" if $verbose;
          print "size: ".$bag_stats->{size}."\n" if $verbose;
          $self->log->info("verified $aippath in ".($bag_end_time-$bag_start_time)."s checktime: ".$bag_stats->{verify_time}." size: ".$bag_stats->{size});
        }
        else {
          $stats->{error}->{count}++;
          $self->log->warn("invalid bag found [$ver_res] : $aippath");
          print "ver_res: [$ver_res] was not 'ok'\n";
          print "found invalid bag at: $aippath\n";
        }
      });
    }

  undef $pool;

  $cv_finish->recv;
  print "total valid bags: ".$stats->{verified}->{count}."\n";
  print "total invalid bags: ".$stats->{error}->{count}."\n";
  $self->log->info("total valid bags: ".$stats->{verified}->{count}." invalid: ".$stats->{error}->{count});
  print "total time: ".(time()-$start_time)."\n";
}


sub next_uid {
    my ($self) = @_;

    my $t_repo  = $self->{t_repo};

    if (! exists $self->{pools}) {
        @{$self->{pools}}=$t_repo->pools();
    }

    # Set up Job queues
    if (! exists $self->{queue}) {
        $self->{queue}={};
        foreach my $pool (@{$self->{pools}}) {
            @{$self->{queue}->{$pool}}=$self->get_pool_queue($pool,$self->limit);
        }
#        print "My Queue: " . Dumper($self->{queue});
    }

    # Set up tried hash (Used to determine which AIPs have already been
    # tried before, so we don't try to do the same AIP twice in same run.
    if (! exists $self->{tried}) {
        $self->{tried}={};
    }
    while (my $pool= shift @{$self->{pools}}) { 
        my $uid = shift @{$self->{queue}->{$pool}};
        if ($uid) {
            $self->{tried}->{$uid}=1;
            # We got a $uid, so put pool at end of pool array
            push @{$self->{pools}}, $pool;
            return $uid;
        } else {
            @{$self->{queue}->{$pool}}=$self->get_pool_queue($pool,$self->limit);
            if (scalar @{$self->{queue}->{$pool}}) {
                # If we got more AIPs, then put this pool to end of pool array
                push @{$self->{pools}}, $pool;
            }
        }
    }
}

sub get_pool_queue {
    my $self = shift;
    my $pool = shift;
    my $limit = shift;
    my $res;
    my $t_repo  = $self->{t_repo};
    my $repository = $t_repo->{reponame};
    my @queue;

    $res = $t_repo->tdrepo->get("/".$t_repo->tdrepo->database."/_design/tdr/_view/repopoolverified?reduce=false&startkey=[\"$repository\",\"$pool\"]&endkey=[\"$repository\",\"$pool\",{}]&limit=$limit",{}, {deserializer => 'application/json'});
    if ($res->code == 200) {
        foreach my $aiplist (@{$res->data->{rows}})  {
            my $uid=$aiplist->{value};
            # Ignore any in list which have already been tried
            if (! exists $self->{tried}->{$uid}) {
                push @queue, $uid;
            }
        }
    } else {
        print STDERR ("get_pool_queue($pool,$limit) GET return code: ".$res->code."\nError: " . $res->error . "\n");
        # Return an empty queue, and try to continue...
    }
    return @queue;
}


1;
