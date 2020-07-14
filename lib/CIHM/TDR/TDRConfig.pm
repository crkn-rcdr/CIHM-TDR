package CIHM::TDR::TDRConfig;
use base qw(Class::Singleton);

use strict;
use Config::General;
use Data::Dumper;
use Log::Log4perl;
use Try::Tiny;
use Log::Log4perl;
use File::Spec;
our %tdrconfig;
my $DEBUG = 0;

=head1 NAME

CIHM::TDR::TDRConfig; - TDR configuration file access

Provides access to the hash returned from Config::General parsing potentially
muliple configuration files.

This is a subclass of Class::Singleton, which allows it to inherit instance(),
and provides a global single instance of the configuration object.

=head1 SYNOPSIS

    my $t_config = CIHM::TDR::TDRConfig->instance($configpath);
      where $configpath can be:
        - a path to a directory that contains a tdr.conf
        - a path to a tdr.conf file
        - undefined, and default of /etc/canadiana/tdr/tdr.conf is read

    $t_config->get_conf($configpath);
      where $configpath can be:
        - a path to a specific tdr configuration file
        - undefined, and the default_config_path is returned

    $t_config->set_conf($configpath);
      Parses and makes available an additional configuration file. 

=cut

sub _new_instance {
    my $class = shift;
    my $self = bless {}, $class;
    Log::Log4perl->init_once("/etc/canadiana/tdr/log4perl.conf");
    $self->{logger} = Log::Log4perl::get_logger("CIHM::TDR");

    my $conf = shift || "/etc/canadiana/tdr/tdr.conf";
    my $conf_file;
    if ( -d $conf ) {
        $conf_file = File::Spec->catpath( $conf, "tdr.conf" );
    }
    elsif ( -f $conf ) {
        $conf_file = $conf;
    }
    else {
        die("invalid config path passed into TDRConfig");
    }
    $self->{default_config_path} = $conf_file;
    $self->{logger}
      ->debug( "default_config_path: " . $self->{default_config_path} );
    my $config = new Config::General( -ConfigFile => $conf_file, );
    $self->{logger}->debug( "default_config: " . Dumper( $config->getall ) )
      if $DEBUG;

    $self->{tdrconfig}->{ $self->{default_config_path} } = { $config->getall };

    return $self;
}

sub logger {
    my ($self) = shift;
    return $self->{logger};
}

sub get_conf {
    my $self = shift;
    $self->{logger}->debug( "default: "
          . Dumper( $self->{tdrconfig}->{ $self->{default_config_path} } ) )
      if $DEBUG;
    my $path =
      shift || return $self->{tdrconfig}->{ $self->{default_config_path} };
    $self->{logger}->debug( "path: " . $path );
    if ( !$self->{tdrconfig}->{$path} ) {
        my $tdrconfig = \$self->set_conf($path);
        $self->{logger}->debug( "tdrconfig: " . Dumper($tdrconfig) ) if $DEBUG;
    }
    return $self->{tdrconfig}->{$path};
}

sub set_conf {
    my $self = shift;
    my $path = shift;
    $self->{logger}->debug("TDRConfig::set_conf path: $path");
    my $config = new Config::General( -ConfigFile => $path );
    $self->{logger}->debug( Dumper($config) ) if $DEBUG;
    my $config_all = { $config->getall() };
    $self->{logger}->debug( "config_all: " . Dumper( $config->getall ) )
      if $DEBUG;
    $self->{tdrconfig}->{$path} = $config_all;

    return $self->{tdrconfig}->{$path};
}

1;
