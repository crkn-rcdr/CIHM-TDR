package CIHM::TDR::Swift;

use strict;
use warnings;

use Carp;
use Try::Tiny;
use CIHM::TDR::TDRConfig;
use CIHM::Swift::Client;
use CIHM::TDR::Repository;
use Archive::BagIt::Fast;
use Data::Dumper;
use File::Find;
use File::Basename;
use File::Path qw(make_path remove_tree);
use DateTime;
use DateTime::Format::ISO8601;
use Digest::MD5;

=head1 NAME

CIHM::TDR::Swift - Managing Canadiana style AIPs within Openstack Swift

=head1 SYNOPSIS

    my $t_repo = CIHM::TDR::Swift->new({config => $configpath});
      where $configpath is as defined in CIHM::TDR::TDRConfig

=cut

sub new {
    my ( $class, $args ) = @_;
    my $self = bless {}, $class;

    if ( ref($args) ne "HASH" ) {
        die "Argument to CIHM::TDR::Replication->new() not a hash\n";
    }
    $self->{args} = $args;

    $self->{tdr_repo} = new CIHM::TDR::Repository(
        {
            configpath => $self->configpath
        }
    );
    my %confighash =
      %{ CIHM::TDR::TDRConfig->instance( $self->configpath )->get_conf };

    # Undefined if no <swift> config block
    if ( exists $confighash{swift} ) {
        my %swiftopt = ( furl_options => { timeout => 120 } );
        foreach ( "server", "user", "password", "account", "furl_options" ) {
            if ( exists $confighash{swift}{$_} ) {
                $swiftopt{$_} = $confighash{swift}{$_};
            }
        }
        $self->{swift}       = CIHM::Swift::Client->new(%swiftopt);
        $self->{swiftconfig} = $confighash{swift};
    }
    else {
        croak "No <swift> configuration block in " . $self->configpath . "\n";
    }

    # Undefined if no <tdrepo> config block
    if ( exists $confighash{tdrepo} ) {
        $self->{tdrepo} = new CIHM::TDR::REST::tdrepo(
            server      => $confighash{tdrepo}{server},
            database    => $confighash{tdrepo}{database},
            type        => 'application/json',
            conf        => $self->configpath,
            repository  => $self->repository,
            clientattrs => { timeout => 3600 },
        );
    }
    else {
        croak "Missing <tdrepo> configuration block in config\n";
    }
    return $self;
}

# Simple accessors for now -- Do I want to Moo?
sub configpath {
    my $self = shift;
    return $self->{args}->{configpath};
}

sub swiftconfig {
    my $self = shift;
    return $self->{swiftconfig};
}

sub repository {
    my $self = shift;
    return $self->swiftconfig->{repository};
}

sub container {
    my $self = shift;
    return $self->swiftconfig->{container};
}

sub tdr_repo {
    my $self = shift;
    return $self->{tdr_repo};
}

sub tdrepo {
    my ($self) = shift;
    return $self->{tdrepo};
}

sub log {
    my $self = shift;
    return $self->tdr_repo->logger;
}

sub swift {
    my $self = shift;
    return $self->{swift};
}

sub since {
    my $self = shift;
    return $self->{args}->{since};
}

sub localdocument {
    my $self = shift;
    return $self->{args}->{localdocument};
}

sub replicationwork {
    my ($self) = @_;

    $self->log->info("Looking for replication work");
    $self->set_replicationwork(
        {
            date          => $self->since,
            localdocument => $self->localdocument,
        }
    );
}

sub set_replicationwork {
    my ( $self, $params ) = @_;
    my ( $res, $code );

    my $newestaips = $self->tdrepo->get_newestaip($params);
    if ( !$newestaips || !scalar(@$newestaips) ) {

        # carp "Nothing new....";
        return;
    }
    foreach my $thisaip (@$newestaips) {
        my $aip = $thisaip->{key};
        my ( $contributor, $identifier ) = split( /\./, $aip );

        # Only set replication for AIPs which are on this repository
        if ( $self->tdr_repo->find_aip_pool( $contributor, $identifier ) ) {
            my $priority = 5;    # Set default priority
            my $match = $self->tdr_repo->aip_match($aip);
            if ( $match && $match->{replicate} ) {
                $priority = $match->{replicate};
            }
            $self->tdrepo->update_item_repository( $aip,
                { replicate => $priority } );
        }
    }
}

sub replicate {
    my ( $self, $options ) = @_;

    my $txtopts = '';
    my $opts = { limit => 1 };
    if ( exists $options->{skip} ) {
        $opts->{skip} = $options->{skip};
        $txtopts .= " skip=" . $opts->{skip};
    }
    my $limit;
    if ( exists $options->{limit} ) {
        $limit = $options->{limit};
        $txtopts .= " limit=$limit";
    }
    $self->log->info("Replicate: $txtopts");

    # One by one, sorted by priority, get the AIPs we should replicate
    my @replicateaips;
    while (( ( !defined $limit ) || $limit > 0 )
        && ( @replicateaips = $self->tdrepo->get_replicate($opts) )
        && scalar(@replicateaips) )
    {
        $limit-- if $limit;
        $self->replicateaip( pop @replicateaips, $options );
    }
}

sub replicateaip {
    my ( $self, $aip, $options ) = @_;

    my $verbose = exists $options->{'verbose'};
    my ( $contributor, $identifier ) = split( /\./, $aip );
    my $aippath = $self->tdr_repo->find_aip_pool( $contributor, $identifier );

    my $updatedoc = {

        # Whatever the outcome, if we update we mark the replication as done.
        replicate => "false"
    };

    if ($aippath) {
        $self->log->info("Replicating $aip to Swift");
        $updatedoc = $self->tdr_repo->get_manifestinfo($aippath);

        # Reset this, cleared by above call
        $updatedoc->{replicate} = "false";

        # Try to copy 3 times before giving up.
        my $success = 0;
        for ( my $tries = 3 ; ( $tries > 0 ) && !$success ; $tries-- ) {
            try {
                $self->bag_upload( $aippath, $aip );
                $success = 1;
            };
        }
        die "Failure while uploading $aip to $aippath\n" if ( !$success );

        my $validate = $self->validateaip( $aip, $options );
        if ( $validate->{'validate'} ) {
            if ( $updatedoc->{'manifest date'} ne $validate->{'manifest date'} )
            {
                carp "Manifest Date Mismatch: "
                  . $updatedoc->{'manifest date'} . " != "
                  . $validate->{'manifest date'}
                  . "\n";    # This shouldn't ever happen
            }
            if ( $updatedoc->{'manifest md5'} ne $validate->{'manifest md5'} ) {
                carp "Manifest MD5 Mismatch: "
                  . $updatedoc->{'manifest md5'} . " != "
                  . $validate->{'manifest md5'}
                  . "\n";    # This shouldn't ever happen
            }
        }
        else {
            $self->log->warn("validation of $aip failed");
            delete $updatedoc->{'manifest date'};
            delete $updatedoc->{'manifest md5'};
        }
    }
    else {
        carp "$aip not found\n";    # This shouldn't ever happen
    }

    # Inform database
    $self->tdrepo->update_item_repository( $aip, $updatedoc );
}

sub replicateaipfrom {
    my ( $self, $aip, $options ) = @_;

    my $verbose = exists $options->{'verbose'};
    my ( $contributor, $identifier ) = split( /\./, $aip );
    my $aippath = $self->tdr_repo->find_aip_pool( $contributor, $identifier );

    my $updatedoc = {};
    if ($aippath) {
        $updatedoc = $self->tdr_repo->get_manifestinfo($aippath);
    }

    # Whatever the outcome, if we update we mark the replication as done.
    $updatedoc->{replicate} = "false";

    my $hasnew = $self->tdr_repo->tdrepo->get_newestaip( { keys => [$aip] } );
    if ( !$hasnew ) {

        # We got an HTTP error code, so exit....
        $self->log->warn("Failed getting 'hasnew' for $aip");
        return;
    }
    if ( !scalar(@$hasnew) ) {

        # Maybe we missed filling in the manifest information?
        # Otherwise increase priority to move AIP from being 'next' again.
        if ( !exists $updatedoc->{'manifest md5'} ) {

            # TODO: Incrementally update priority to 9 before going to
            # letter....
            $updatedoc->{priority} = "a";
            $self->log->warn("Can't find source for $aip");
        }
        else {
            # TODO:  Compare files on disk to possible existing database
            $self->log->info("$aip already existed on server");
        }
        $self->tdr_repo->tdrepo->update_item_repository( $aip, $updatedoc );
        return;
    }
    my @repos = @{ $hasnew->[0]->{value}[1] };

    my $copyrepo;
    foreach my $thisrepo (@repos) {
        if ( $thisrepo eq $self->repository ) {
            $copyrepo = $thisrepo;
            last;
        }
    }
    if ( !$copyrepo ) {

# This won't happen in regular operation, but might happen if replication topology changed.
        $self->log->info("Couldn't find repo to copy $aip");

# Not a serious problem.  Update document to no longer be replicated, and go on to the next.
        $self->tdr_repo->tdrepo->update_item_repository( $aip, $updatedoc );
        return;
    }
    my $aipinfo = $self->get_aipinfo($aip);
    if ( !$aipinfo ) {
        $self->log->info("Couldn't get AIP information from $copyrepo\n");
        return;
    }
    if (   $aipinfo->{'manifest md5'}
        && $updatedoc->{'manifest md5'}
        && $aipinfo->{'manifest md5'} eq $updatedoc->{'manifest md5'} )
    {
        $self->log->info( "$aip with md5("
              . $updatedoc->{'manifest md5'}
              . ") already exists.\n" );
        $self->tdr_repo->tdrepo->update_item_repository( $aip, $updatedoc );
        return;
    }

    # Find existing, or create new, path to be used in incoming.
    my $incomingpath;
    my ( $pool, $path, $id ) =
      $self->tdr_repo->find_incoming_pool( $contributor, $identifier );
    if ( !$pool ) {
        my $pool = $self->tdr_repo->pool_free;
        if ( !$pool ) {

            # This should never happen, unless something misconfigured
            print STDERR "Couldn't get pool with free space\n";
            return;
        }
        $incomingpath = $self->tdr_repo->incoming_basepath($pool) . "/$aip";
        mkdir $incomingpath;
    }
    else {
        $incomingpath = $path . "/" . $id;
    }

    $self->log->info("Replicating $aip from Swift");

    # Try to copy 3 times before giving up.
    my $success = 0;
    for ( my $tries = 3 ; ( $tries > 0 ) && !$success ; $tries-- ) {
        try {
            $self->bag_download( $aip, $incomingpath );
            $success = 1;
        }
        catch {
            my $errmessage = "bag_download($aip) error: $_";
            print STDERR $errmessage . "\n";
            $self->log->warn($errmessage);
        };
    }

    if ($success) {
        $success = 0;
        try {
            my $bagit = new Archive::BagIt::Fast($incomingpath);
            my $valid = $bagit->verify_bag();

            # If we have the size, then set it in database
            if ( $bagit->{stats} && $bagit->{stats}->{size} ) {
                $updatedoc->{'filesize'} = $bagit->{stats}->{size};
            }

            # If it was valid, mark current datetime as last validation
            if ($valid) {
                $updatedoc->{'verified'} = 'now';
            }
            $success = $valid;
        };
        if ( !$success ) {
            my $errmessage = "Error verifying bag: $incomingpath";
            print STDERR $errmessage . "\n";
            $self->log->warn($errmessage);
        }
    }
    else {
        my $errmessage = "Error copying $aip to $incomingpath";
        print STDERR $errmessage . "\n";
        $self->log->warn($errmessage);
    }
    if ( !$success ) {

# Set priority to letter, which keeps in _view/replicate , but won't be part of replication
        $updatedoc->{priority} = "a";
        $updatedoc->{filesize} = "";
        $self->tdr_repo->tdrepo->update_item_repository( $aip, $updatedoc );
        return;
    }

    # Ensure the success, size, etc is recorded
    $self->tdr_repo->tdrepo->update_item_repository( $aip, $updatedoc );

    # If the AIP already exists in the repository....
    if ( exists $updatedoc->{'manifest md5'} ) {
        $self->log->info("Removing existing $aip revision");

        if ( !( $self->tdr_repo->aip_delete( $contributor, $identifier ) ) ) {
            my $errmessage = "Failed to remove AIP: $aip";
            print STDERR $errmessage . "\n";
            $self->log->warn($errmessage);

# Set priority to letter, which keeps in _view/replicate , but won't be part of replication
            $updatedoc->{priority} = "a";
            $updatedoc->{filesize} = "";
            $self->tdr_repo->tdrepo->update_item_repository( $aip, $updatedoc );
            return;
        }
    }

    $self->log->info("Adding $aip");

    if (
        !( $self->tdr_repo->aip_add( $contributor, $identifier, $updatedoc ) ) )
    {
        my $errmessage = "Failed to add AIP: $aip";
        print STDERR $errmessage . "\n";
        $self->log->warn($errmessage);

# Set priority to letter, which keeps in _view/replicate , but won't be part of replication
        $updatedoc->{priority} = "a";
        $updatedoc->{filesize} = "";
        $self->tdr_repo->tdrepo->update_item_repository( $aip, $updatedoc );
        return;
    }
}

sub bag_upload {
    my ( $self, $source, $prefix ) = @_;

    $prefix =~ s!/*$!/!;    # Add a trailing slash
    $source =~ s!/*$!/!;    # Add a trailing slash

    # List local files
    my @bagfiles;
    find( sub { -f && -r && push @bagfiles, $File::Find::name; }, $source );

    #print Dumper ($source,$prefix,\@bagfiles);

    # To support BAG updates, check what files already exist
    my %containeropt = ( "prefix" => $prefix );
    my %bagdata;

    # Need to loop possibly multiple times as Swift has a maximum of
    # 10,000 names.
    my $more = 1;
    while ($more) {
        my $bagdataresp =
          $self->swift->container_get( $self->container, \%containeropt );
        if ( $bagdataresp->code != 200 ) {
            croak "container_get("
              . $self->container
              . ") for $prefix returned "
              . $bagdataresp->code . " - "
              . $bagdataresp->message . "\n";
        }
        $more = scalar( @{ $bagdataresp->content } );
        if ($more) {
            $containeropt{'marker'} =
              $bagdataresp->content->[ $more - 1 ]->{name};

            foreach my $object ( @{ $bagdataresp->content } ) {
                my $file = substr $object->{name}, ( length $prefix );
                $bagdata{$file} = $object;
            }
        }
        undef $bagdataresp;
    }

    # Load manifest to get MD5 of data files.
    my $bagfile = $source . "manifest-md5.txt";
    open( my $fh, '<:raw', $bagfile )
      or die "replicate_aip: Could not open file '$bagfile' $!\n";
    chomp( my @lines = <$fh> );
    close $fh;
    foreach my $line (@lines) {
        if ( $line =~ /^\s*([^\s]+)\s+([^\s]+)\s*/ ) {
            my ( $md5, $file ) = ( $1, $2 );
            if ( exists $bagdata{$file} ) {

                # Fill in md5 from manifest to compare before sending
                $bagdata{$file}{'md5'} = $md5;
            }
        }
    }

    #print Dumper (\%bagdata);

    # looping through filenames found on filesystem.
    foreach my $bagfile (@bagfiles) {
        my $file = substr $bagfile, ( length $source );
        my $object = $prefix . $file;

        # Check if file with same md5 already on Swift
        if (   !exists $bagdata{$file}
            || !exists $bagdata{$file}{'md5'}
            || $bagdata{$file}{'md5'} ne $bagdata{$file}{'hash'} )
        {

            open( my $fh, '<:raw', $bagfile )
              or die "bag_upload: Could not open file '$bagfile' $!\n";

            my $filedate = "unknown";
            my $mtime    = ( stat($fh) )[9];
            if ($mtime) {
                my $dt = DateTime->from_epoch( epoch => $mtime );
                $filedate = $dt->datetime . "Z";
            }

            my $putresp =
              $self->swift->object_put( $self->container, $object, $fh,
                { 'File-Modified' => $filedate } );
            if ( $putresp->code != 201 ) {
                die(    "object_put of $object returned "
                      . $putresp->code . " - "
                      . $putresp->message
                      . "\n" );
            }
            close $fh;
        }

        # Remove key, to allow detection of extra files in Swift
        delete $bagdata{$file};
    }

    if ( keys %bagdata ) {

        # These files existed on Swift, but not on disk, so delete
        # (Files with different names in different AIP revision)
        foreach my $key ( keys %bagdata ) {
            my $delresp =
              $self->swift->object_delete( $self->container,
                $bagdata{$key}{'name'} );
            if ( $delresp->code != 204 ) {
                $self->log->warn( "object_delete of "
                      . $bagdata{$key}{'name'}
                      . " returned "
                      . $delresp->code . " - "
                      . $delresp->message );
            }
        }
    }
}

sub bag_download {
    my ( $self, $prefix, $destination ) = @_;

    $prefix =~ s!/*$!/!;         # Add a trailing slash
    $destination =~ s!/*$!/!;    # Add a trailing slash

    if ( !-d $destination ) {
        die "Filesystem path '$destination' not directory\n";
    }

    # Have list of files already in destination.
    my %destfiles;
    find(
        sub {
            if ( -f && -r ) {
                $destfiles{ substr $File::Find::name, ( length $destination ) }
                  = 1;
            }
        },
        $destination
    );

    # Get list of objects in Swift
    my %containeropt = ( "prefix" => $prefix );
    my %bagdata;

    # Need to loop possibly multiple times as Swift has a maximum of
    # 10,000 names.
    my $more = 1;
    while ($more) {
        my $bagdataresp =
          $self->swift->container_get( $self->container, \%containeropt );
        if ( $bagdataresp->code != 200 ) {
            croak "container_get("
              . $self->container
              . ") for $prefix returned "
              . $bagdataresp->code . " - "
              . $bagdataresp->message . "\n";
        }
        $more = scalar( @{ $bagdataresp->content } );
        if ($more) {
            $containeropt{'marker'} =
              $bagdataresp->content->[ $more - 1 ]->{name};

            foreach my $object ( @{ $bagdataresp->content } ) {
                my $file = substr $object->{name}, ( length $prefix );
                $bagdata{$file} = $object;
            }
        }
    }

    foreach my $bagfilename ( keys %bagdata ) {
        my $destfilename = $destination . $bagfilename;
        if ( exists $destfiles{$bagfilename} ) {
            delete $destfiles{$bagfilename};

            my $size = ( stat($destfilename) )[7];

            ## If size matches, check md5
            if ( $size == $bagdata{$bagfilename}{'bytes'} ) {
                open FILE, "$destfilename";
                my $ctx = Digest::MD5->new;
                $ctx->addfile(*FILE);
                my $hash = $ctx->hexdigest;
                close(FILE);
                if ( $hash eq $bagdata{$bagfilename}{'hash'} ) {

                    # If size and MD5 match, skip to next file
                    next;
                }
            }
        }
        my $objectname = $prefix . $bagfilename;
        my $object = $self->swift->object_get( $self->container, $objectname );
        if ( $object->code != 200 ) {
            croak "object_get container: '"
              . $self->container
              . "' , object: '$objectname'  returned "
              . $object->code . " - "
              . $object->message . "\n";
        }
        my ( $fn, $dirs, $suffix ) = fileparse($destfilename);
        make_path($dirs);
        open( my $fh, '>:raw', $destfilename )
          or die "Could not open file '$destfilename' $!";
        print $fh $object->content;
        close $fh;
        my $filemodified = $object->object_meta_header('File-Modified');
        if ($filemodified) {
            my $dt = DateTime::Format::ISO8601->parse_datetime($filemodified);
            if ( !$dt ) {
                die "Couldn't parse ISO8601 date from $filemodified\n";
            }
            my $atime = time;
            utime $atime, $dt->epoch(), $destfilename;
        }
    }
    foreach my $delfile ( keys %destfiles ) {
        my $file = "$destination$delfile";
        unlink $file or die "Could not unlink $file: $!";
    }
}

sub get_aipinfo {
    my ( $self, $aip ) = @_;

    my $aipinfo = $self->tdrepo->get_item_otherrepo( $aip, $self->repository );
    if ( !$aipinfo ) {
        return {};
    }
    return $aipinfo;
}

# Options include: 'skip:i','timelimit:i','limit:i','verbose','aip:s'
sub validate {
    my ( $self, $options ) = @_;

    if ( exists $options->{aip} ) {
        $self->validateaip( $options->{aip}, $options );
    }
    else {

        my $start_time    = time();
        my $validatecount = 0;
        my $errorcount    = 0;

        $self->log->info("Running validation at: $start_time");
        my $repotxt = "reduce=false&startkey=[\"swift\"]&endkey=[\"swift\",{}]";
        if ( $options->{limit} ) {
            $repotxt .= "&limit=" . $options->{limit};
        }
        if ( $options->{skip} ) {
            $repotxt .= "&skip=" . $options->{skip};
        }

        my $res = $self->tdrepo->get(
            "/"
              . $self->tdrepo->database
              . "/_design/tdr/_view/repopoolverified?$repotxt",
            {},
            { deserializer => 'application/json' }
        );

        if ( $res->code != 200 ) {
            croak "repopoolverified for validate returned "
              . $res->code . " - "
              . $res->error . "\n";
        }
        my @aiplist;
        foreach my $aipdoc ( @{ $res->data->{rows} } ) {
            push @aiplist, $aipdoc->{value};
        }
        undef $res;

        foreach my $aip (@aiplist) {
            if ( exists $options->{timelimit}
                && ( time() - $start_time ) > $options->{timelimit} )
            {
                last;
            }
            my $val = $self->validateaip( $aip, $options );
            if ( $val->{validate} ) {
                $validatecount++;
                $self->log->info("verified Swift AIP: $aip");
            }
            else {
                $errorcount++;
                $self->log->warn("invalid Swift AIP: $aip");
                print "invalid Swift AIP: $aip\n";
            }
        }
        print "total valid bags: $validatecount\n";
        print "total invalid bags: $errorcount\n";
        $self->log->info(
            "total valid bags: $validatecount invalid: $errorcount");
        print "total time: " . ( time() - $start_time ) . "\n";
    }
}

sub validateaip {
    my ( $self, $aip, $options ) = @_;

    my $verbose = exists $options->{'verbose'};

    my %passlist = map { $_ => 1 } (
        "bag-info.txt",       "bagit.txt",
        "manifest-md5.txt",   "tagmanifest-md5.txt",
        "manifest-crc32.txt", "tagmanifest-crc32.txt"
    );

    # Assume validated unless problem found
    my %return = (
        "validate" => 1,
        "filesize" => 0
    );

    my %containeropt = ( "prefix" => $aip . "/" );
    my %aipdata;

    # Need to loop possibly multiple times as Swift has a maximum of
    # 10,000 names.
    my $more = 1;
    while ($more) {
        my $aipdataresp =
          $self->swift->container_get( $self->container, \%containeropt );
        if ( $aipdataresp->code != 200 ) {
            warn "container_get("
              . $self->container
              . ") for $aip/data/ for validate_aip returned "
              . $aipdataresp->code . " - "
              . $aipdataresp->message . "\n";
            return {};
        }
        $more = scalar( @{ $aipdataresp->content } );
        if ($more) {
            $containeropt{'marker'} =
              $aipdataresp->content->[ $more - 1 ]->{name};

            foreach my $object ( @{ $aipdataresp->content } ) {
                my $file = substr $object->{name}, ( length $aip ) + 1;
                $aipdata{$file} = $object;
            }
        }
        undef $aipdataresp;
    }
    my $manifest = $aip . "/manifest-md5.txt";
    my $aipmanifest = $self->swift->object_get( $self->container, $manifest );
    if ( $aipmanifest->code != 200 ) {
        warn "validate_aip container: '"
          . $self->container
          . "' , object: '$manifest'  returned "
          . $aipmanifest->code . " - "
          . $aipmanifest->message . "\n";
        return {};
    }
    $return{'manifest date'} =
      $aipmanifest->object_meta_header('File-Modified');
    $return{'manifest md5'} = $aipmanifest->etag;
    my @lines = split /\n/, $aipmanifest->content;
    foreach my $line (@lines) {
        if ( $line =~ /^\s*([^\s]+)\s+([^\s]+)\s*/ ) {
            my ( $md5, $file ) = ( $1, $2 );
            if ( exists $aipdata{$file} ) {
                $return{filesize} += $aipdata{$file}{'bytes'};
                if ( $aipdata{$file}{'hash'} ne $md5 ) {
                    print "MD5 mismatch: "
                      . Dumper( $file, $md5, $aipdata{$file} )
                      if $verbose;
                    $return{validate} = 0;
                }
                $aipdata{$file}{'checked'} = 1;
            }
            else {
                print "File '$file' missing from Swift\n"
                  if $verbose;
                $return{validate} = 0;
            }
        }
    }
    $manifest = $aip . "/tagmanifest-md5.txt";
    $aipmanifest = $self->swift->object_get( $self->container, $manifest );
    if ( $aipmanifest->code == 200 ) {

        $return{'tagmanifest date'} =
          $aipmanifest->object_meta_header('File-Modified');
        $return{'tagmanifest md5'} = $aipmanifest->etag;
        my @lines = split /\n/, $aipmanifest->content;
        foreach my $line (@lines) {
            if ( $line =~ /^\s*([^\s]+)\s+([^\s]+)\s*/ ) {
                my ( $md5, $file ) = ( $1, $2 );
                if ( exists $aipdata{$file} ) {
                    $return{filesize} += $aipdata{$file}{'bytes'};
                    if ( $aipdata{$file}{'hash'} ne $md5 ) {
                        print "MD5 mismatch: "
                          . Dumper( $file, $md5, $aipdata{$file} )
                          if $verbose;
                        $return{validate} = 0;
                    }
                    $aipdata{$file}{'checked'} = 1;
                }
                else {
                    print "File '$file' missing from Swift\n"
                      if $verbose;
                    $return{validate} = 0;
                }
            }

        }
    }
    elsif ( $aipmanifest->code != 404 )
    {    # Not found is valid for older bags -- for now...

        warn "validate_aip container: '"
          . $self->container
          . "' , object: '$manifest'  returned "
          . $aipmanifest->code . " - "
          . $aipmanifest->message . "\n";
        return {};
    }

    foreach my $key ( keys %aipdata ) {
        if ( !exists $aipdata{$key}{'checked'} && !$passlist{$key} ) {
            $return{validate} = 0;
            print "File '$key' is extra in Swift\n" if ($verbose);
        }
    }

    if ( $return{validate} ) {

        # Update CouchDB...
        $self->tdrepo->update_item_repository(
            $aip,
            {
                'verified' => 'now',
                'filesize' => $return{filesize}
            }
        );
    }
    print Dumper ( \%return ) if $verbose;
    return \%return;
}

sub walk {
    my ( $self, $options ) = @_;

    $| = 1;
    print "Loading AIP list from CouchDB database...";

    # Get list from CouchDB
    my $res = $self->tdrepo->get(
        "/"
          . $self->tdrepo->database
          . "/_design/tdr/_list/manifestinfo/tdr/repoown?reduce=false&startkey=[\"swift\"]&endkey=[\"swift\",{}]&include_docs=true",
        {}, {}
    );
    if ( $res->code != 200 ) {
        print STDERR ( "Walk tdrepo->get return code: "
              . $res->code
              . "\nError: "
              . $res->error
              . "\n" );
    }
    if ( $res->failed ) {
        die(    "Walk tdrepo->get failed flag set\n"
              . $res->response->as_string()
              . "\n" );
    }
    if ( !keys %{ $res->data } ) {
        die(    "Walk tdrepo->get empty hash\n"
              . $res->response->as_string()
              . "\n" );
    }
    my $aiplist = $res->data;

    # Count of AIPs with each storage
    my %counts = (
        swift => 0,
        couch => scalar( keys %{$aiplist} )
    );

    print " " . $counts{couch} . " found in DB.\n";

    # Walk though AIP list in Swift
    my %containeropt = ( delimiter => "/" );

    # Need to loop possibly multiple times as Swift has a maximum of
    # 10,000 names.
    my $more = 1;
    while ($more) {
        my $aipdataresp =
          $self->swift->container_get( $self->container, \%containeropt );
        if ( $aipdataresp->code != 200 ) {
            croak "container_get("
              . $self->container
              . ") returned "
              . $aipdataresp->code . " - "
              . $aipdataresp->message . "\n";
        }
        $more = scalar( @{ $aipdataresp->content } );
        if ($more) {
            $containeropt{'marker'} =
              $aipdataresp->content->[ $more - 1 ]->{subdir};

            foreach my $object ( @{ $aipdataresp->content } ) {
                my $aip = substr $object->{subdir}, 0, -1;
                $self->walk_aip( $options, $aip, $aiplist, \%counts );
            }
        }
    }

    my $aiplistcount = keys %{$aiplist};
    my $nomanifest   = 0;
    if ( !$options->{quiet} && ( $aiplistcount > 0 ) ) {
        print
"There were $aiplistcount AIPs only found in DB. Listing those with manifests:\n--begin--\n";
        foreach my $aipkey ( keys %{$aiplist} ) {
            if ( exists $aiplist->{$aipkey}->{'manifest md5'} ) {
                print "$aipkey\n";
            }
            else {
                $nomanifest++;
            }
        }
        print "--end--\n";
        if ($nomanifest) {
            print
"There were $nomanifest AIPs without manifests (most likely being replicated)\n";
        }
    }

    print $counts{swift}
      . " AIPS found in Swift, "
      . $counts{couch}
      . " in CouchDB database.\n";
}

sub walk_aip {
    my ( $self, $options, $aip, $aiplist, $counts ) = @_;

    my $count = ( $counts->{swift} )++;

    my $aipres =
      $self->swift->object_head( $self->container, "$aip/manifest-md5.txt" );
    if ( $aipres->code != 200 ) {
        warn "walk_aip container:: '"
          . $self->container
          . "' , object: '$aip/manifest-md5.txt'  returned "
          . $aipres->code . " - "
          . $aipres->message . "\n";
        return;
    }

    my $update    = 0;
    my $updatedoc = {
        'manifest md5'  => $aipres->etag,
        'manifest date' => $aipres->object_meta_header('file-modified')
    };

    if ( exists $aiplist->{$aip} ) {
        if ( !exists $aiplist->{$aip}->{'manifest md5'} ) {

    # Initialize variable -- will be noticed as mismatch, but without PERL error
            $aiplist->{$aip}->{'manifest md5'} = '[unset]';
        }
        if ( !exists $aiplist->{$aip}->{'manifest date'} ) {

    # Initialize variable -- will be noticed as mismatch, but without PERL error
            $aiplist->{$aip}->{'manifest date'} = '[unset]';
        }
        if (
            $aiplist->{$aip}->{'manifest md5'} ne $updatedoc->{'manifest md5'} )
        {
            $update = 1;
            if ( !$options->{quiet} ) {
                print "MD5 mismatch $aip: "
                  . $aiplist->{$aip}->{'manifest md5'} . " != "
                  . $updatedoc->{'manifest md5'} . "\n";
            }
        }
        elsif ( $aiplist->{$aip}->{'manifest date'} ne
            $updatedoc->{'manifest date'} )
        {
            $update = 1;
            if ( !$options->{quiet} ) {
                print "Date mismatch $aip: "
                  . $aiplist->{$aip}->{'manifest date'} . " != "
                  . $updatedoc->{'manifest date'} . "\n";
            }
        }
        delete $aiplist->{$aip};

    }
    else {
        if ( !$options->{quiet} ) {
            print "New Swift AIP found: $aip\n";
        }
        $update = 1;
    }

    if ( $update && $options->{update} ) {
        if ( !$options->{quietupdate} ) {
            print "Updating: "
              . Data::Dumper->new( [$updatedoc], [$aip] )->Dump . "\n";
        }
        $self->tdr_repo->update_item_repository( $aip, $updatedoc );
    }
}

1;
