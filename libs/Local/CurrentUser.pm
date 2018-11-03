#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
#

=begin

Begin-Doc
Name: Local::CurrentUser
Type: module
Description: CurrentUser Detection Routine
Comments: 

End-Doc

=cut

package Local::CurrentUser;
require Exporter;
use strict;
use Local::UsageLogger;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw( Local_CurrentUser );

BEGIN {
    &LogAPIUsage();
}

my $cached_curuid;
my $cached_curuser;

# Begin-Doc
# Name: Local_CurrentUser
# Type: function
# Description: Returns detected userid
# Syntax: $env = &Local_CurrentUser()
# Comments: returns userid executing script forced to lowercase
# End-Doc
sub Local_CurrentUser {
    my $user = $cached_curuser;

    if ( $user && $cached_curuid != $< ) {
        $user = undef;
    }

    # Cache results to avoid repeated getpwuid calls for same uid in same app invocation
    if ( !defined($user) ) {
        $cached_curuid = $<;
        if ( $^O !~ /Win/ ) {
            eval { $user = ( getpwuid($cached_curuid) )[0]; };
        }
        if ($user) {
            $cached_curuser = lc $user;
        }
    }

    if ( !defined($user) ) {
        $user = lc $ENV{USERNAME};
    }

    return $user;
}

1;
