
=begin

Begin-Doc
Name: Local::Env
Type: module
Description: UMR Environment Detection Routine
Comments: 

End-Doc

=cut

package Local::Env;
require Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw( Local_Env );

our $detected_env;

# Begin-Doc
# Name: Local_Env
# Type: function
# Description: Returns detected environment name
# Syntax: $env = &Local_Env()
# Comments: returns one of 'prod', 'test', or 'dev'
# End-Doc
sub Local_Env {
    if ( !$detected_env ) {
        if ( $ENV{HTTP_HOST} =~ /-test\./ ) {
            $detected_env = "test";
        }
        elsif ( $ENV{HTTP_HOST} =~ /-dev\./ ) {
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
