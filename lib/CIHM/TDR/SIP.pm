package CIHM::TDR::SIP;
use strict;
use warnings;

use File::Basename;
use File::Glob;
use List::Util;
use MIME::Types;
use XML::LibXML;
use XML::LibXSLT;
use File::Temp qw/ tempfile tempdir /;
use File::Spec;
use Cwd;
use Try::Tiny;
use Archive::BagIt::Fast;

=head1 NAME

CIHM::TDR::SIP - Submission Information Package tools

=head1 SYNOPSIS

    my $sip = CIHM::TDR::SIP->new('/path/to/sip');
    $sip->validate($strict);
    my $identifier = $sip->identifier;

=cut

sub new {
    my ( $self, $root ) = @_;
    my $sip = {};
    die("No SIP root directory specified\n") unless ( $root && -d $root );
    $sip->{root} = $root;

    # Parse the metadata records and create an Xpath context so we can do
    # namespace-enabled Xpath queries on it
    $sip->{metadata} = XML::LibXML->new->parse_file("$root/data/metadata.xml");
    $sip->{metadata_xc} = XML::LibXML::XPathContext->new( $sip->{metadata} );
    $sip->{metadata_xc}->registerNs( 'mets',  "http://www.loc.gov/METS/" );
    $sip->{metadata_xc}->registerNs( 'xlink', "http://www.w3.org/1999/xlink" );

    # Where to find various resource files
    $sip->{resource} = "/opt/xml/current/";

    return bless($sip);
}

sub identifier {
    my ($self) = @_;
    return $self->{metadata_xc}->findvalue("/mets:mets/\@OBJID");
}

# Added $mytemp as PDF files being split can take a fair bit of space.
sub validate {
    my ( $self, $strict, $mytemp ) = @_;

    if ( !$mytemp ) {
        $mytemp = File::Spec->tmpdir;
    }

    try {
        my $bagit = new Archive::BagIt::Fast( $self->{root} );
        die("Bagit version is not 0.97\n") unless $bagit->version() == 0.97;
        my $valid = $bagit->verify_bag();
    }
    catch {
        die "invalid SIP @ " . $self->{root} . ": $_\n";
    };

    # Validate the core METS document against the METS schema.
    my $mets_schema;
    try {
        $mets_schema = XML::LibXML::Schema->new( location =>
              join( "/", $self->{resource}, "unpublished/xsd/mets.xsd" ) );
    }
    catch {
        die "Caught error while creating new schema: $_\n";
    };
    try {
        $mets_schema->validate( $self->{metadata} );
    }
    catch {
        die "Caught error while validating METS: $_\n";
    };

    # Validate the METS document against the Canadiana METS profile. (This will
    # not necessarily catch all requirements, but does a pretty good job
    # of getting all the technical ones.)
    my $xslt      = XML::LibXSLT->new();
    my $cmets_xml = XML::LibXML->new->parse_file(
        join( "/", $self->{resource}, "unpublished/xsl/cmets.xsl" ) );
    my $cmets_stylesheet = $xslt->parse_stylesheet($cmets_xml);
    my $cmets_validated  = $cmets_stylesheet->transform( $self->{metadata} );
    if ( $cmets_validated->findnodes('/cmetsValidation/error') ) {
        die( "Canadiana METS profile validation errors:\n"
              . $cmets_validated->toString(1) );
    }

    # Validate the records embedded in the dmdSec.
    foreach my $dmdsec (
        $self->{metadata_xc}->findnodes("descendant::mets:dmdSec/mets:mdWrap") )
    {
        my $embedded    = ( $dmdsec->findnodes("mets:xmlData/child::*") )[0];
        my $MDTYPE      = $dmdsec->getAttribute("MDTYPE");
        my $OTHERMDTYPE = $dmdsec->getAttribute("OTHERMDTYPE");
        if ( $MDTYPE eq "MARC" ) {
            try {
                $self->validate_node( $embedded,
                    "unpublished/xsd/MARC21slim.xsd" );
            }
            catch {
                die "METS validation of embedded MARC: $_\n";
            };
        }
        elsif ( $MDTYPE eq "DC" ) {
            try {
                $self->validate_node( $embedded,
                    "unpublished/xsd/simpledc.xsd" );
            }
            catch {
                die "METS validation of embedded Dublin Core: $_\n";
            };
        }
        elsif ( $MDTYPE eq "OTHER" && $OTHERMDTYPE eq "txtmap" ) {
            try {
                $self->validate_node( $embedded, "published/xsd/txtmap.xsd" );
            }
            catch {
                die "METS validation of embedded Txtmap: $_\n";
            };
        }
        elsif ( $MDTYPE eq "OTHER" && $OTHERMDTYPE eq "issueinfo" ) {
            try {
                $self->validate_node( $embedded,
                    "published/xsd/issueinfo.xsd" );
            }
            catch {
                die "METS validation of embedded Issueinfo: $_\n";
            };
        }
        else {
            die(
"Found dmdSec with unsupported metadata type:  MDTYPE=\'$MDTYPE\" OTHERMDTYPE=\"$OTHERMDTYPE\"\n"
            );
        }
    }

    # Check for the existence of the files in the METS record against the
    # files in the archive.
    foreach my $file (
        $self->{metadata_xc}->findnodes(
"/mets:mets/mets:fileSec/mets:fileGrp[\@USE='master' or \@USE='distribution']/mets:file"
        )
      )
    {
        my $mime = $file->getAttribute('MIMETYPE');
        my $id   = $file->getAttribute('ID');
        my $href =
          $self->{metadata_xc}->findvalue(
"/mets:mets/mets:fileSec/mets:fileGrp/mets:file[\@ID='$id']/mets:FLocat[\@LOCTYPE='URN']/\@xlink:href"
          );
        my $filename = join( '/', $self->{root}, "data", "files", $href );
        if ( !-f $filename ) {
            die("METS record references missing file $filename\n");
        }
        my $filemime = ( MIME::Types::by_suffix($filename) )[0];
        if ( $filemime ne $mime ) {
            die(
"METS record asserts that $filename should have MIME type $mime, but file seems to have $filemime\n"
            );
        }
    }

    # Check the files in the archive for naming convention.
    foreach my $file ( File::Glob::bsd_glob("$self->{root}/data/files/*") ) {
        my $filename = basename($file);
        die(
"$file contains invalid characters. Only A-Za-z0-9_.- are permitted\n"
        ) unless ( $filename =~ /^[\w\.-]+$/ );

        if (
            !$self->{metadata_xc}->findnodes(
"/mets:mets/mets:fileSec/mets:fileGrp/mets:file/mets:FLocat[\@xlink:href = '$filename']"
            )
          )
        {
            die("Found extraneous file $file\n") if ($strict);
        }
    }

    # Check the image files in the archive.
    foreach my $file ( File::Glob::bsd_glob("$self->{root}/data/files/*") ) {

        my $suffix_mime = ( MIME::Types::by_suffix($file) )[0];
        my $suffix_type = uc($1) if $suffix_mime =~ /.*\/(.*)/;
        die "$file extension isn't supported\n"
          unless grep $suffix_type, ( 'TIFF', 'JPEG', 'JP2', 'PDF', 'XML' );

        if ( $suffix_type eq 'PDF' ) {
            my $tempdir =
              tempdir( "CIHM::SIPXXXXXX", CLEANUP => 1, DIR => $mytemp );

            my @command = (
                "/usr/bin/pdfseparate",    # TODO: in config file?
                $file, $tempdir . "/%d.pdf"
            );
            system(@command) == 0
              or die "PDF separation @command failed: $?\n";

            my @pdffiles;

            if ( opendir( my $dh, $tempdir ) ) {
                while ( readdir $dh ) {
                    next if -d $_;
                    push @pdffiles, $_;
                }
            }
            else {
                die "Can't open $tempdir for listing\n";
            }

            foreach my $pdffile (@pdffiles) {

                # Using ImageMagic's identify to verify file type
                my $magic_type =
                  qx(identify -format "%m" $tempdir/$pdffile 2> /dev/null);
                die
"Distribution PDF page $pdffile type doesn't match extension\n"
                  unless $magic_type =~ /$suffix_type/;
            }
        }
        elsif ( $suffix_type eq 'XML' ) {

            # TODO: Validate the XML file to one of the specific schemas
            my $xml = XML::LibXML->new->parse_file("$file");
            my $xpc = XML::LibXML::XPathContext->new($xml);
            $xpc->registerNs( 'txt',
                'http://canadiana.ca/schema/2012/xsd/txtmap' );
            $xpc->registerNs( 'alto',
                'http://www.loc.gov/standards/alto/ns-v3' );
            if (   $xpc->exists( '//txt:txtmap', $xml )
                || $xpc->exists( '//txtmap', $xml ) )
            {
                my $schema =
                  XML::LibXML::Schema->new(
                    location => "$self->{resource}/published/xsd/txtmap.xsd" );
                try {
                    $schema->validate($xml);
                }
                catch {
                    die "Caught error while validating $file as txtmap: $_\n";
                };
            }
            elsif ($xpc->exists( '//alto', $xml )
                || $xpc->exists('//alto:alto'), $xml )
            {
                my $schema = XML::LibXML::Schema->new( location =>
                      "$self->{resource}/unpublished/xsd/alto-3-1.xsd" );
                try {
                    $schema->validate($xml);
                }
                catch {
                    die "Caught error while validating $file as ALTO: $_\n";
                };
            }
            else {
                die "Unknown XML schema for $file\n";
            }
        }
        else {
            # Using ImageMagic's identify to verify file type
            my $magic_type = qx(identify -format "%m" $file 2> /dev/null);
            die "$file type doesn't match extension\n"
              unless $magic_type =~ /$suffix_type/;

        }
    }

    return 1;
}

# Turn $node into a new XML document and validate it against the schema in
# file $xsd
sub validate_node {
    my ( $self, $node, $xsd ) = @_;
    my $record = $node->cloneNode(1);
    my $doc    = XML::LibXML::Document->new;
    $doc->adoptNode($record);
    $doc->setDocumentElement($record);
    my $schema =
      XML::LibXML::Schema->new( location => "$self->{resource}/$xsd" );
    $schema->validate($doc);
    return 1;
}

1;
