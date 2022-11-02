#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
#

=begin

Begin-Doc
Name: Local::HTMLImpersonate
Type: module
Description: Standardized user impersonation code
Comments:

Use the following as a bookmarklet in your browser for easily switching users. The HTMLImpersonate call should
be placed in your code as early as possible. I generally would recommend putting it right after GetRequest, so
that all users of REMOTE_USER in the code are covered.

Bookmarklet (Prompts for user): <A HREF="javascript:imuser=prompt(&quot;Username&quot;,&quot;&quot;);document.cookie=&quot;REMOTE_USER_IMPERSONATE=&quot;+escape(imuser)+&quot;;domain=.&quot;+escape(document.domain)+&quot;path=/&quot;;location=location;">Impersonate</A>

JavaScript (Prompts for user): javascript:imuser=prompt("Username","");document.cookie="REMOTE_USER_IMPERSONATE="+escape(imuser)+";domain=."+escape(document.domain)+";path=/";location=location;

JavaScript (Hardwired to particular user): javascript:document.cookie="REMOTE_USER_IMPERSONATE=specificuser;domain=."+escape(document.domain)+";path=/";location=location;

Example:

use Local::HTMLUtil;
use Local::HTMLImpersonate;

&HTMLGetRequest();
&HTMLContentType();

&HTMLImpersonate("myapp:allowimpersonate");

# If you are doing your own access controls/checks for who is allowed to impersonate and don't want to check code
# &HTMLImpersonateAlways();

&do_stuff();

End-Doc

=cut

package Local::HTMLImpersonate;
require Exporter;
use strict;

use Local::PrivSys;
use Local::HTMLUtil;
use Local::UsageLogger;
use Sys::Syslog;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw(
    HTMLImpersonate
    HTMLImpersonateAlways
);

BEGIN {
    &LogAPIUsage();
}

# Begin-Doc
# Name: HTMLImpersonate
# Type: function
# Syntax: &HTMLImpersonate("priv:code");
# Description: Checks for a REMOTE_USER_IMPERSONATE cookie and redefines REMOTE_USER if the user has priv code
#
# Comment: Will be a no-op if the REMOTE_USER_IMPERSONATE environment variable is already defined. (To prevent reentrant requests)
#
# End-Doc
sub HTMLImpersonate {
    my $privcode = shift;

    if ( !$privcode ) {
        return;
    }

    if ( !$ENV{REMOTE_USER} ) {
        return;
    }

    if ( $ENV{REMOTE_USER_IMPERSONATE} ) {
        return;
    }

    my %cookies = &HTMLGetCookies();
    if ( !$cookies{REMOTE_USER_IMPERSONATE} ) {
        return;
    }

    my $newuser  = $cookies{REMOTE_USER_IMPERSONATE};
    my $realuser = $ENV{REMOTE_USER};
    if ( &PrivSys_CheckPriv( $ENV{REMOTE_USER}, $privcode ) ) {
        $ENV{REMOTE_USER_IMPERSONATE} = $newuser;
        $ENV{REMOTE_USER_REAL}        = $realuser;
        $ENV{REMOTE_USER}             = $newuser;

        # write out a log entry here to record the impersonation for tracking purposes
        syslog "debug", "HTMLImpersonate: %s impersonating %s for application %s via %s", $realuser, $newuser, $0, $privcode;
    }
    else {

        # write out a log entry here to record the impersonation for tracking purposes
        syslog "debug", "HTMLImpersonate: %s denied impersonation of %s for application %s via %s", $realuser, $newuser, $0, $privcode;
    }

    return;
}

# Begin-Doc
# Name: HTMLImpersonateAlways
# Type: function
# Syntax: &HTMLImpersonateAlways();
# Description: Checks for a REMOTE_USER_IMPERSONATE cookie and redefines REMOTE_USER if the user has priv code
#
# Comment: Will be a no-op if the REMOTE_USER_IMPERSONATE environment variable is already defined. (To prevent reentrant requests)
#
# End-Doc
sub HTMLImpersonateAlways {
    if ( !$ENV{REMOTE_USER} ) {
        return;
    }

    if ( $ENV{REMOTE_USER_IMPERSONATE} ) {
        return;
    }

    my %cookies = &HTMLGetCookies();
    if ( !$cookies{REMOTE_USER_IMPERSONATE} ) {
        return;
    }

    my $newuser  = $cookies{REMOTE_USER_IMPERSONATE};
    my $realuser = $ENV{REMOTE_USER};

    $ENV{REMOTE_USER_IMPERSONATE} = $newuser;
    $ENV{REMOTE_USER_REAL}        = $realuser;
    $ENV{REMOTE_USER}             = $newuser;

    # write out a log entry here to record the impersonation for tracking purposes
    syslog "debug", "HTMLImpersonateAlways: %s impersonating %s for application %s", $realuser, $newuser, $0;

    return;
}

1;
