
=begin

Begin-Doc
Name: Local::SetUID
Type: module
Description: Handy utility module for selectively changing uid for scripts run as root
Comments: No security exposure
End-Doc

=cut

package Local::SetUID;
require Exporter;
use strict;
use Local::UsageLogger;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw(
    SetUID
);

BEGIN {
    &LogAPIUsage();
}

# Begin-Doc
# Name: SetUID
# Description: switch real and effective uid to a particular user if we are running as root only
# Syntax: &SetUID("user")
# Comments: only for scripts that might be run by system admin as root on a server, or run intentionally as root
# Comments: Also functions as a "if script isn't running as this user, terminate" sanity check
# End-Doc
sub SetUID {
    my $target_user = shift;
    my $target_uid;
    my $target_gid;

    if ( int($target_user) eq $target_user ) {
        $target_uid = int($target_user);
        $target_gid = int($target_user);
    }
    else {
        my @tmp = getpwnam($target_user);
        if ( $tmp[0] ne $target_user ) {
            die "Unable to look up target userid ($target_user).\n";
        }
        $target_uid = $tmp[2];
        $target_gid = $tmp[3];
    }

    if ( $target_uid == 0 ) {
        die "Unable to look up ($target_user) for SetUID operation.\n";
    }

    #
    # If we are running as root, switch to thar id
    #
    if ( $< == 0 ) {
        $) = $target_gid;
        $( = $target_gid;
        $> = $target_uid;
        $< = $target_uid;
    }

    if ( $< != $target_uid || $> != $target_uid ) {
        print "Unable to successfully set UID.\n";
        print "Found ($</$>) wanted ($target_uid/$target_uid). Exiting!\n";
        die;
    }

    if ( $) != $target_gid || $( != $target_gid ) {
        print "Unable to successfully set GID.\n";
        print "Found ($(/$)) wanted ($target_gid/$target_gid). Exiting!\n";
        die;
    }

    return;
}

1;
