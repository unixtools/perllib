
=begin

Begin-Doc
Name: Local::HTMLImpersonate
Type: module
Description: Standardized user impersonation code
Comments:

Use the following as a bookmarklet in your browser for easily switching users. The HTMLImpersonate call should
be placed in your code as early as possible. I generally would recommend putting it right after GetRequest, so
that all users of REMOTE_USER in the code are covered.

NOTE: Be sure and update the bookmarklet with an appropriate base domain for the cookies.

Bookmarklet (Prompts for user): <A HREF="javascript:imuser=prompt(&quot;Username&quot;,&quot;&quot;);document.cookie=&quot;REMOTE_USER_IMPERSONATE=&quot;+escape(imuser)+&quot;;domain=.mst.edu;path=/&quot;;location=location;">Impersonate</A>

JavaScript (Prompts for user): javascript:imuser=prompt("Username","");document.cookie="REMOTE_USER_IMPERSONATE="+escape(imuser)+";domain=.mst.edu;path=/";location=location;

JavaScript (Hardwired to particular user): javascript:document.cookie="REMOTE_USER_IMPERSONATE=specificuser;domain=.mst.edu;path=/";location=location;

Example:

use Local::HTMLUtil;
use Local::HTMLImpersonate;

&HTMLGetRequest();
&HTMLContentType();

&HTMLImpersonate("myapp:allowimpersonate");

&do_stuff();

End-Doc

=cut

package Local::HTMLImpersonate;
require 5.000;
require Exporter;
use strict;

eval "use Local::PrivSys;";
use Local::HTMLUtil;
use Sys::Syslog;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw(
  HTMLImpersonate
);

# Begin-Doc
# Name: HTMLImpersonate
# Type: function
# Syntax: &HTMLImpersonate("priv:code");
# Description: Checks for a REMOTE_USER_IMPERSONATE cookie and redefines REMOTE_USER if user has priv code
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
        syslog "debug",
          "HTMLImpersonate: %s impersonating %s for application %s",
          $realuser, $newuser, $0;
    }
    else {

  # write out a log entry here to record the impersonation for tracking purposes
        syslog "debug",
          "HTMLImpersonate: %s denied impersonation of %s for application %s",
          $realuser, $newuser, $0;
    }

    return;
}

1;
