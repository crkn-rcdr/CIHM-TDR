package CIHM::TDR::REST::tdrepo;

use strict;
use Carp;
use Data::Dumper;
use DateTime;
use JSON;

use Moo;
with 'Role::REST::Client';
use Types::Standard qw(HashRef Str Int Enum HasMethods);

=head1 NAME

CIHM::TDR::REST::tdrepo - Subclass of Role::REST::Client used to
interact with "tdrepo" CouchDB database

=head1 SYNOPSIS

    my $t_repo = CIHM::TDR::REST::tdrepo->new($args);
      where $args is a hash of arguments.  In addition to arguments
      processed by Role::REST::Client we have the following 

      $args->{conf} is as defined in CIHM::TDR::TDRConfig
      $args->{database} is the Couch database name.
      $self->{repository} is the name of the TDR repository (Example: "toma")

=cut


sub BUILD {
    my $self = shift;
    my $args = shift;

    $self->{LocalTZ} = DateTime::TimeZone->new( name => 'local' );
    $self->{conf} = $args->{conf}; 
    $self->{repository} = $args->{repository};
    $self->{database} = $args->{database};
}

# Simple accessors for now -- Do I want to Moo?
sub database {
    my $self = shift;
    return $self->{database};
}

sub update_item_repository {
  my ($self, $uid, $updatedoc) = @_;
  my ($res, $code, $data);

  my $repository = $self->{repository};
  my $id = $uid . "|item_repository." . $repository;

  # This encoding makes $updatedoc variables available as form data
  $self->type("application/x-www-form-urlencoded");
  $res = $self->post("/".$self->{database}."/_design/tdr/_update/itemrepo/".$id, $updatedoc);

  if ($res->code != 201 && $res->code != 200) {
      warn "_update/itemrepo/$id POST return code: " . $res->code . "\n";
  }
}

sub get_item_otherrepo {
  my ($self, $uid, $otherrepo) = @_;
  my ($res, $code, $data);

  my $id = $uid . "|item_repository." . $otherrepo;

  # This encoding makes $updatedoc variables available as form data
  $self->type("application/json");
  $res = $self->get("/".$self->database."/$id",{}, {deserializer => 'application/json'});
  if ($res->code == 200) {
      return $res->data;
  } else {
      warn "get_item_otherrepo GET return code: " . $res->code . "\n";
  }
  return;
}


sub update_verified_date {
  my ($self, $uid, $date) = @_;
  my ($res, $updatedoc);

  if ($date) {
      $updatedoc = { 'verified date' => $date };
  } else {
      $updatedoc = { 'verified' => 'now' };
  }
  $self->update_item_repository($uid,$updatedoc);
}

sub update_item {
    my $self = shift;
    my $baginfo = shift;
    my ($res, $code, $data);

    my $uid = $baginfo->{uid};
    my $LocalTZ = $self->{LocalTZ};
    my $repository = $self->{repository};

    my $itemdirty=0;
    my $item = {};
    $res = $self->get("/".$self->{database}."/$uid",{}, {deserializer => 'application/json'});
    if ($res->code == 200) {
        $item=$res->data;
    }
    elsif ($res->code == 404) {
        # Build the type=item
        $item->{'type'} = 'item';
        $itemdirty=1;
    }
    else {
        warn "item GET return code: ".$res->code."\n";
        return;
    }

    if (!$item->{'created_at'}) {
        # Everything going into Couch will be in GMT
        my $dt = DateTime->now;
        $item->{'created_at'} = $dt->datetime."Z";
        $itemdirty=1;
    }
    if (!$item->{'created_by'}) {
        $item->{'created_by'} = "CIHM::TDR::REST::tdrepo for repository: $repository";
        $itemdirty=1;
    }
    if (exists($item->{'ingest date'})) {
        delete $item->{'ingest date'};
        $itemdirty=1;
    }
    if ($itemdirty) {
        # TODO: For now put it, but later do a proper update...
        $res = $self->put("/".$self->{database}."/$uid", $item);

        if ($res->code != 201) {
            warn "item PUT return code: ".$res->code."\n";
        }
    }
}

sub get_recent_adddate_keys {
    my ($self, $params) = @_;
    my ($res, $code, $data, $recentdate, $docrev);

    my $startkey="[]";
    my $date = $params->{date};
    my $localdocument = $params->{localdocument};


    # If we have a local document, grab the previous values
    if ($localdocument) {
        $self->type("application/json");
        $res = $self->get("/".$self->{database}."/_local/".$localdocument,{},{deserializer => 'application/json'});
        if ($res->code == 200) {
            $docrev = $res->data->{"_rev"};
            $startkey = to_json($res->data->{"latestkey"});
        }
    }

    # A $data parameter will override the $startkey from a local document
    if ($date) {
        if ($date =~ /(\d\d\d\d)(\-\d\d|)(\-\d\d|)(T\d\d|)/ ) {
            # Accepts an rfc3339 style date, and grabs the yyyy-mm-ddThh part
            # The month, day, and hour are optional.
            my $year=$1;
            my $month=substr($2||"000",1);
            my $day=substr($3 || "000",1); 
            my $hour=substr($4 || "000",1);
            $startkey = sprintf("[\"%04d\",\"%02d\",\"%02d\",\"%02d\"]",
                                $year,$month,$day,$hour);
        } elsif ($date =~ /(\d+)\s*hours/) {
            # Accepts a number of hours to be subtracted from current GMT time
            my $dt = DateTime->now()->subtract( hours => $1);
            $startkey = sprintf("[\"%04d\",\"%02d\",\"%02d\",\"%02d\"]",
                                $dt->year(),$dt->month(),$dt->day(),$dt->hour());
        } else {
            warn "get_recent_adddate_keys() - invalid {date}=$date\n";
            # Didn't provide valid date, so return null
            return;
        }
    }

    # If we have a local document, grab the currently highest date key,
    # and store for next run.
    if ($localdocument) {
        $res = $self->get("/".$self->{database}."/_design/tdr/_view/adddate", { reduce => 'false', descending => 'true', limit => '1' },{deserializer => 'application/json'});
        if ($res->code == 200) {
            if ($res->data->{rows} && $res->data->{rows}[0]->{key}) {
                my $latestkey = $res->data->{rows}[0]->{key};
                pop(@$latestkey); # pop off the (alphabetically sorted) AIP

                my $newdoc = { latestkey => $latestkey};
                if ($docrev) {
                    $newdoc->{"_rev"} = $docrev;
                }

                $self->type("application/json");
                $res = $self->put("/".$self->{database}."/_local/".$localdocument, $newdoc);
                if ($res->code != 201 && $res->code != 200) {
                    warn "_local/$localdocument PUT return code: " . $res->code . "\n";
                }
            }
        }
    }

    $res = $self->get("/".$self->{database}."/_design/tdr/_list/itemdatekey/tdr/adddate", { reduce => 'false', startkey => $startkey, endkey => '[{}]' },{deserializer => 'application/json'});
    if ($res->code == 200) {
        # If the same AIP is modified multiple times within the time given,
        # it would otherwise show up multiple times..
        use List::MoreUtils qw(uniq);
        my @uniqaip = uniq(@{$res->data});
        return (\@uniqaip);
    }
    else {
        warn "_list/itemdatekey/tdr/adddate GET return code: ".$res->code."\n"; 
        return;
    }
}

=head2 get_newestaip($params)

Uses _design/tdr/_list/newtome/tdr/newestaip to get a list of servers which
have the latest revision of an AIP.

Parameter is a hash of possible parameters
  date - A date (as defined with get_recent_adddate_keys()
  localdocument - a local CouchDB document where last found date stored
  keys - An array of keys to look up
  repository - overrides the default repository taken from the config file
=cut
sub get_newestaip {
    my ($self, $params) = @_;
    my ($res, $code);
    my $restq = {};

    if ((!$params->{date} || $params->{date} ne 'all') && 
        ($params->{date} || $params->{localdocument})) {
        my $recentuids = $self->get_recent_adddate_keys($params);
        if ($recentuids && scalar(@$recentuids)) {
            $restq->{keys}=$recentuids;
        } else {
            # We asked for items since a date and got none, so do nothing else
            return;
        }
    }
    if ($params->{keys}) {
        $restq->{keys}=$params->{keys};
    }
    my $repository=$self->{repository};
    if (exists $params->{repository}) {
        $repository=$params->{repository};
    }

    $self->type("application/json");
    $res = $self->post("/".$self->{database}."/_design/tdr/_list/newtome/tdr/newestaip?group=true&me=$repository",$restq, {deserializer => 'application/json'});
    if ($res->code == 200) {
        if (defined $res->data->{rows}) {
            return $res->data->{rows};
        } else {
            return [];
        }
    }
    else {
        warn "_view/newestaip GET return code: ".$res->code."\n"; 
        return;
    }
}

sub get_repostats {
    my ($self) = @_;
    my ($res, $code);
    my $restq = {};

    $self->type("application/json");
    $res = $self->get("/".$self->{database}."/_design/tdr/_view/repoown?group_level=1",{}, {deserializer => 'application/json'});
    if ($res->code == 200) {
        return $res->data->{rows};
    }
    else {
        warn "_view/repoown GET return code: ".$res->code."\n"; 
        return;
    }
}

sub get_replicate {
    my ($self, $params) = @_;
    my ($res);


    $self->type("application/json");
    my $limit='';
    if ($params->{limit}) {
        $limit="&limit=".$params->{limit};
    }

    $res = $self->get("/".$self->{database}."/_design/tdr/_view/replicate?reduce=false&startkey=\[\"".$self->{repository}."\"\]&endkey=\[\"".$self->{repository}."\",\"999\"\]$limit",{}, {deserializer => 'application/json'});
    if ($res->code == 200) {
        my @aips=();
        foreach my $aip (@{$res->data->{rows}}) {
            push(@aips,$aip->{key}[2]);
        }
        return(@aips);
    }
    else {
        warn "tdr/_view/replicate GET return code: ".$res->code."\n";
        return;
    }
}

1;
