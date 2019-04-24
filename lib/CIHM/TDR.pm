package CIHM::TDR;
use strict;
use warnings;

use CIHM::TDR::SIP;
use CIHM::TDR::TDRConfig;
use CIHM::TDR::Repository;
use File::Copy;
use File::Copy::Recursive qw(dircopy);
use File::Glob qw(bsd_glob);
use File::Path qw(make_path remove_tree);
use POSIX qw(strftime);
use String::CRC32;
use XML::LibXML;
use Archive::BagIt;

=head1 NAME

CIHM::TDR - Trustworthy Digital Repository management tools

=head1 SYNOPSIS

    my $tdr = CIHM::TDR->new('/path/to/tdr.conf');
       - Path parsed as defined by CIHM::TDR::TDRConfig
    $tdr->ingest($contributor_code, $sip_root);
    $tdr->update_medatata($contributor, $metadata);
    $tdr->changelog($message);
    $tdr->export_sip($contributor, $identifier, $target);

=cut

=head1 VERSION

Version 0.13

=cut

our $VERSION = '0.13';


sub new {
    my($self, $tdr_configpath) = @_;
    my $tdr = {};
    $tdr->{conf} = CIHM::TDR::TDRConfig->instance($tdr_configpath);
    $tdr->{repo} = CIHM::TDR::Repository->new($tdr_configpath);
    return bless($tdr);
}

sub repo {
    my $self = shift;
    return $self->{repo};
}

sub ingest {
    my($self, $contributor, $sip_root, $pool) = @_;
    my $sip = CIHM::TDR::SIP->new($sip_root);
    $sip->validate(1);
    my $id  = $sip->identifier;

    my $t_repo  = $self->repo;
    # If pool not valid or not given, use the pool with the most free space
    if (!($t_repo->pool_valid($pool))) {
        $pool = $t_repo->pool_free;
    }

    my $aip = $t_repo->find_aip_pool($contributor, $id);
    my $aip_trashcan  = $t_repo->find_trash_pool($contributor, $id);
    my $aip_incoming = $t_repo->incoming_aippath($pool,"$contributor.$id");

    die("Found existing trashcan directory $aip_trashcan") if ($aip_trashcan);
    die("Found existing incoming directory $aip_incoming") if (-d $aip_incoming);

    if ($aip) {
        my $revision_name = strftime("%Y%m%dT%H%M%S", gmtime(time));
        dircopy($aip, $aip_incoming) or die("Failed to copy $aip to $aip_incoming: $!");
        if (-d "$aip_incoming/data/sip") {
            move("$aip_incoming/data/sip", "$aip_incoming/data/revisions/$revision_name") or
                die("Failed to move $aip_incoming/data/sip to $aip_incoming/data/revisions/$revision_name: $!");
            dircopy($sip_root, "$aip_incoming/data/sip") or die("Failed to copy content of $sip to $aip_incoming/data/sip: $!");
            $self->changelog($aip_incoming, "Updated SIP; old SIP stored as revision $revision_name");
        }
        else {
            dircopy($sip_root, "$aip_incoming/data/sip") or die("Failed to copy content of $sip to $aip_incoming/data/sip: $!");
            $self->changelog($aip_incoming, "Created new SIP in existing AIP");
        }
    }
    else {
        make_path("$aip_incoming/data/sip") or die("Failed to create $aip_incoming/data/sip: $!");
        make_path("$aip_incoming/data/revisions") or die("Failed to create $aip_incoming/data/revisions: $!");
        File::Copy::Recursive::rcopy_glob("$sip_root/*", "$aip_incoming/data/sip") or die("Failed to copy content of $sip to $aip_incoming/data/sip: $!");
        $self->changelog($aip_incoming, "Created new AIP");
    }

    # Generate BagIt information files for the AIP
    Archive::BagIt->make_bag($aip_incoming);
    
    # Move the new or updated AIP into place
    $t_repo->aip_delete($contributor,$id) or die("Failed to move $aip to $aip_trashcan: $!");
    $t_repo->aip_add($contributor,$id) or die("Failed to add $aip_incoming to repository: $!");
    return 1;
}

=head2 ingest_check($contributor, $identifier, $update, $metadatamd5)

Pre-ingest checks to determine if the environment is set correctly.

=cut
sub ingest_check {
    my($self, $contributor, $identifier, $update, $metadatamd5) = @_;

    # Look up to determine if this is an update of existing AIP
    my $aip = $self->repo->find_aip_pool($contributor, $identifier);
    if ($aip && !$update) {
        die("AIP exists but --update not requested: $aip\n");
    }
    if (!$aip && $update) {
        die("--update requested, but AIP missing from local repository for ". $contributor. ".". $identifier."\n");
    }

    # Additional checks if "tdrepo" database configured
    if ($self->repo->tdrepo) {
        my $newestaip=$self->repo->tdrepo->get_newestaip({
            keys => [ "$contributor.$identifier" ],
            repository => ""
                                                        });
        if( defined $newestaip->[0]) {
            my $tdrepo_mandate=$newestaip->[0]->{value}[0];
            if ($aip) {
                my $mtime=(stat($aip."/manifest-md5.txt"))[9];
                if ($mtime) {
                    my $dt = DateTime->from_epoch(epoch => $mtime);
                    my $file_mandate=$dt->datetime. "Z";

                    if ($tdrepo_mandate ne $file_mandate) {
                        die("Date of $aip/manifest-md5.txt doesn't match <tdrepo> database: $file_mandate != $tdrepo_mandate");
                    }
                } else {
                    die ("Can't stat $aip/manifest-md5.txt");
                }
            } else {
                die("AIP in <tdrepo> database but not in local repository");
            }
        } elsif ($update) {
            die("--update requested, but AIP missing from <tdrepo> database for ". $contributor. ".". $identifier);
        }
    }

    if ($metadatamd5 && $aip) {
        open my $fh,"<",$aip."/manifest-md5.txt"
            or die("Can't open manifest file within $aip");
        while (my $line = <$fh>) {
            chomp($line);
            if (substr($line,-12) eq 'metadata.xml') {
                my ($md5,$filename)=split /\s+/, $line;
                if($md5 eq $metadatamd5) {
                    die("metadata.xml from SIP matches existing AIP: $aip/$filename\n");
                }
            }
        }
        close ($fh);
    }
}

=head2 update_metadata($contributor, $identifier, $metadata)

Replace the existing metadata for the specified document with new metadta.
Creates a partial revision without replacing the digital objects
themselves.

=cut
sub update_metadata {
    my($self, $contributor, $identifier, $metadata, $reason,$pool) = @_;
    my $t_repo  = $self->repo;
    # If pool not valid or not given, use the pool with the most free space
    if (!($t_repo->pool_valid($pool))) {
        $pool = $t_repo->pool_free;
    }

    my $aip = $t_repo->find_aip_pool($contributor, $identifier);
    my $aip_trashcan  = $t_repo->find_trash_pool($contributor, $identifier);
    my $aip_incoming = $t_repo->incoming_aippath($pool,"$contributor.$identifier");

    die("No such AIP: $contributor.$identifier") unless ($aip);
    die("Found existing trashcan  directory $aip_trashcan") if ($aip_trashcan);
    die("Found existing incoming directory $aip_incoming") if (-d $aip_incoming);


    # Save the existing metadata as a partial revision
    my $revision_name = strftime("%Y%m%dT%H%M%S.partial", gmtime(time));
    dircopy($aip, $aip_incoming) or die("Failed to copy $aip to $aip_incoming: $!");
    mkdir("$aip_incoming/data/revisions/$revision_name") or
        die("Failed to create $aip_incoming/revisions/$revision_name: $!");
    move("$aip_incoming/data/sip/data/metadata.xml", "$aip_incoming/data/revisions/$revision_name/metadata.xml") or
        die("Failed tp move $aip_incoming/data/sip/data/metadata.xml to $aip_incoming/data/revisions/$revision_name/metadata.xml: $!");
    copy($metadata, "$aip_incoming/data/sip/data/metadata.xml") or
        die("Failed to copy $metadata to $aip_incoming/data/revisions/$revision_name/metadata.xml: $!");
    copy("$aip_incoming/data/sip/manifest-md5.txt",  "$aip_incoming/data/revisions/$revision_name/manifest-md5.txt") or
        die("Failed to copy $aip_incoming/data/sip/manifest-md5.txt to $aip_incoming/data/revisions/$revision_name/manifest-md5.txt: $!");

    # Update the SIP bagit info
    Archive::BagIt->make_bag("$aip_incoming/data/sip");

    # Create the new SIP
    my $revision = CIHM::TDR::SIP->new("$aip_incoming/data/sip");
    $revision->validate(1);
    

    # Log what we updated
    $self->changelog($aip_incoming, "Updated metadata record. Reason: $reason");

    # Generate BagIt information files for the AIP
    Archive::BagIt->make_bag($aip_incoming);
    
    # Move the new or updated AIP into place
    $t_repo->aip_delete($contributor,$identifier) or die("Failed to move $aip to $aip_trashcan: $!");
    $t_repo->aip_add($contributor,$identifier) or die("Failed to add $aip_incoming to repository: $!");
    return 1;
}

sub export_sip {
    my($self, $contributor, $identifier, $target) = @_;
    my $aip = $self->repo->find_aip_pool($contributor, $identifier);
    die("AIP not found: $contributor.$identifier") unless ($aip);
    my $sip = "$aip/data/sip";
    die("No SIP for $aip") unless (-d $sip);
    dircopy($sip, $target) or die("Failed to copy $sip to $target: $!");
}

sub export_revision {
    my($self, $contributor, $identifier, $revision, $target) = @_;
    my $aip = $self->repo->find_aip_pool($contributor, $identifier);
    die("AIP not found: $contributor.$identifier") unless ($aip);
    my $sip = "$aip/data/revisions/$revision";
    die("No SIP for $aip") unless (-d $sip);
    dircopy($sip, $target) or die("Failed to copy $sip to $target: $!");
}

# Export a copy of the DMD record for the root item in the structmap.
sub export_dmd {
    my($self, $contributor, $identifier, $target) = @_;
    my $aip = $self->repo->find_aip_pool($contributor, $identifier);
    die("AIP not found: $contributor.$identifier") unless ($aip);
    my $metadata = "$aip/data/sip/data/metadata.xml";
    die("No metatada in SIP for $aip") unless (-f $metadata);
    my $doc = XML::LibXML->new->load_xml(location => $metadata);
    my $dmdid = $doc->findvalue('/mets:mets/mets:structMap/mets:div/@DMDID');
    my($record) = $doc->findnodes("/mets:mets/mets:dmdSec[\@ID='$dmdid']");
    open(OUT, ">$target") or die("Can't open $target for writing: $!");
    print(OUT $record->toString(1));
    close(OUT);
}

# Remove any leftover staging or backup directories for the AIP
sub clean {
    my($self, $contributor, $identifier) = @_;
    my $t_repo  = $self->repo;

    while (my $incomingpath = $t_repo->find_incoming_pool($contributor,$identifier))
    {
	print "Removing: $incomingpath\n";
	remove_tree ($incomingpath) or die("Failed to remove $incomingpath: $!");
    }
    while (my $trashpath = $t_repo->find_trash_pool($contributor,$identifier))
    {
	print "Removing: $trashpath\n";
	remove_tree ($trashpath) or die("Failed to remove $trashpath: $!");
    }
}

# Delete the SIP from an AIP
sub delete {
    my($self, $contributor, $identifier, $pool, $reason) = @_;
    my $t_repo  = $self->repo;
    # If pool not valid or not given, use the pool with the most free space
    if (!($t_repo->pool_valid($pool))) {
	$pool = $t_repo->pool_free;
    }

    my $aip = $t_repo->find_aip_pool($contributor, $identifier);
    my $aip_trashcan  = $t_repo->find_trash_pool($contributor, $identifier);

    my $aip_incoming = $t_repo->incoming_aippath($pool,"$contributor.$identifier");

    die("No such AIP: $contributor.$identifier") unless ($aip);
    die("Found existing trashcan  directory $aip_trashcan") if ($aip_trashcan);

    die("Found existing incoming directory $aip_incoming") if (-d $aip_incoming);

    my $revision_name = strftime("%Y%m%dT%H%M%S", gmtime(time));
    dircopy($aip, $aip_incoming) or die("Failed to copy $aip to $aip_incoming: $!");
    foreach my $file (bsd_glob("$aip_incoming/data/revisions/*"), "$aip_incoming/data/sip", "$aip_incoming/data/cmr.xml") {
        if (-d $file) {
            remove_tree($file) or die("Failed to remove $file: $!");
        }
        elsif (-f $file) {
            unlink($file) or die("Failed to remove $file: $!");
        }
    }
    $self->changelog($aip_incoming, "Deleted SIP and all revisions from archive. Reason: $reason");

    # Generate BagIt information files for the AIP
    Archive::BagIt->make_bag($aip_incoming);
    
    # Move the new or updated AIP into place
    $t_repo->aip_delete($contributor,$identifier) or die("Failed to move $aip to $aip_trashcan: $!");
    $t_repo->aip_add($contributor,$identifier) or die("Failed to add $aip_incoming to repository: $!");
    return 1;
}

# Append a changelog record
sub changelog {
    my($self, $aip, $message) = @_;
    open(CHANGELOG, ">>$aip/data/changelog.txt") or die("Failed to open $aip/data/changelog.txt for writing: $!");
    print(CHANGELOG strftime("%FT%TZ", gmtime(time)) . "  $message\n");
    close(CHANGELOG);
    return 1;
}

# Return the contributor code of the selected AIP root
sub contributor {
    my($self, $aip) = @_;
    my @element = split(/\/+/, $aip);
    my @contrib = split(/\./, $element[-1]);
    return $contrib[-2];
}

# Return the identifier for the selected AIP root.
sub identifier {
    my($self, $aip) = @_;
    my @path = split(/\/+/, $aip);
    my @identifier = split(/\./, $path[-1]);
    return $identifier[1];
}

1;
