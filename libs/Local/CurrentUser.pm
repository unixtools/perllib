
=begin

Begin-Doc
Name: Local::CurrentUser
Type: module
Description: CurrentUser Detection Routine
Comments: 

End-Doc

=cut

package Local::CurrentUser;
require 5.000;
require Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw( Local_CurrentUser );

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
        eval { $user = ( getpwuid($cached_curuid) )[0]; };
        $cached_curuser = lc $user;
    }

    if ( !defined($user) ) {
        $user = lc $ENV{USERNAME};
    }

    return $user;
}

1;
