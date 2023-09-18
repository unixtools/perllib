#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
#

=begin

Begin-Doc
Name: Local::Env
Type: module
Description: Environment Detection Routine
Comments: 

End-Doc

=cut

package Local::Env;
require Exporter;
use strict;
use Local::UsageLogger;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw( Local_Env );

BEGIN {
    &LogAPIUsage();
}

our $detected_env;

# Begin-Doc
# Name: Local_Env_Reset
# Type: function
# Description: Resets detected environment name
# Syntax: &Local_Env_Reset()
# End-Doc
sub Local_Env_Reset {
    undef $detected_env;
}

# Begin-Doc
# Name: Local_Env
# Type: function
# Description: Returns detected environment name
# Syntax: $env = &Local_Env()
# Comments: returns one of 'prod', 'test', or 'dev'
# End-Doc
sub Local_Env {
    if ( !$detected_env ) {
        if ( $ENV{LOCAL_ENV} ) {
            $detected_env = $ENV{LOCAL_ENV};
        }
        elsif ( $ENV{HTTP_HOST} && $ENV{HTTP_HOST} =~ /-test\./ ) {
            $detected_env = "test";
        }
        elsif ( $ENV{HTTP_HOST} && $ENV{HTTP_HOST} =~ /-dev\./ ) {
            $detected_env = "dev";
        }
        else    # try to determine based on hostname of local machine
        {
            my $hn;
            eval { use Sys::Hostname; $hn = hostname; };
            my $shn = $hn;
            $shn =~ s/\..*$//go;

            if ( $shn =~ /-d\d+$/ ) {
                $detected_env = "dev";
            }
            elsif ( $shn =~ /-t\d+$/ ) {
                $detected_env = "test";
            }
            elsif ( $shn =~ /-p\d+$/ ) {
                $detected_env = "prod";
            }
            else    # assume production as fallback case
            {
                $detected_env = "prod";
            }
        }
    }

    return $detected_env;
}

1;
