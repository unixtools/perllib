#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: http://svn.unixtools.org/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#


=pod

Begin-Doc
Name: Local::SimpleRPC
Type: module
Description: object to manage our simplified RPC implementation
Comments: This is a wrapper module that includes both Local::SimpleRPC::Client and Local::SimpleRPC::Server
End-Doc

=cut

package Local::SimpleRPC;
require 5.000;
require Exporter;
use strict;

# Stub routines - autoloader only, used for easy referencing of remote routines
package Local::SimpleRPC::Client::Stub;
use JSON;
use Carp;
use LWP::UserAgent;
use URI::Escape;
use strict;
our $AUTOLOAD;

# Begin-Doc
# Name: AUTOLOAD
# Type: function
# Description: perl internal/magic AUTOLOAD function that implements the RPC call
# Syntax: Never called directly, perl calls when a RPC client issues $rpc->RPCMethod(@args)
# Comments: This routine implements the RPC encoding of request, error checking, and response to caller
# End-Doc
sub AUTOLOAD {
    my $self = shift;
    my $type = ref($self)
        or croak "$self is not an object";
    my @args = @_;

    $self->{error} = undef;

    my $debug = $self->{debug};

    my $name = $AUTOLOAD;
    $name =~ s/.*://;    # strip fully-qualified portion

    $debug && print "AutoLoad called with $self / $type / $name\n";

    my $url = $self->{base_url} || croak "no base url provided";
    $url =~ s|/*$||go;
    $url .= "/" . $name;

    if ( $self->{url_suffix} ) {
        $url .= $self->{url_suffix};
    }

    $debug && print "Submit to URL: $url\n";

    my $ua = new LWP::UserAgent;
    $ua->timeout(30);
    $ua->agent("MSTSimpleRPC/1.0");

    my $req = HTTP::Request->new( POST => $url );
    if ( $self->{user} && $self->{password} ) {
        print "submitting with user and password\n";
        $req->authorization_basic( $self->{user}, $self->{password} );
    }
    $req->content_type("application/x-www-form-urlencoded");

    # Standard is that args is a 'hash in array form'
    my @content_pieces;
    while ( scalar(@args) ) {
        my $a = shift @args;
        if ( scalar(@args) > 0 ) {
            my $b = shift @args;
            push( @content_pieces,
                      URI::Escape::uri_escape($a) . "="
                    . URI::Escape::uri_escape($b) );
        }
        else {
            push( @content_pieces, URI::Escape::uri_escape($a) );
        }
    }

    # Fill with request parms first
    my $req_content = join( "&", @content_pieces );
    $debug && print "content request = $req_content\n";
    $req->content($req_content);

    my $res = $ua->request($req);

    if ( !$res ) {
        $self->{error} = "Error performing LWP request.";
        croak $self->{error};
    }

    # get response
    if ( !$res->is_success ) {
        $self->{error} = "LWP Request Failed: " . $res->message;
        croak $self->{error};
    }

    my $content = $res->content;
    if ( !$content ) {
        $self->{error} = "No content returned from LWP request.";
        croak $self->{error};
    }

    $debug && print "content: $content\n";

    my $jsonret;
    eval { $jsonret = from_json($content); };

    if ($@) {
        $self->{error} = "Error parsing JSON response: " . $@;
        croak $self->{error};
    }

    if ( !$jsonret ) {
        $self->{error} = "JSON response not found.";
        croak $self->{error};
    }

    if ( ref($jsonret) ne "ARRAY" ) {
        $self->{error} = "Invalid response, not a JSON array ($jsonret)";
        croak $self->{error};
    }

    my ( $status, $msg, @results ) = @$jsonret;
    if ( $status != 0 ) {
        $self->{error} = "Error returned from API: " . $msg;
        croak $self->{error};
    }

    return @results;
}

# Begin-Doc
# Name: DESTROY
# Type: method
# Description: perl internal/magic DESTROY method
# Syntax: N/A
# Comments: Placeholder to counteract AUTOLOAD during module destruction.
# End-Doc
sub DESTROY {

    # Do nothing, otherwise it tries to autoload it.
}

# Client component
package Local::SimpleRPC::Client;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use Data::Dumper;
use Local::AuthSrv;
use Carp;

@ISA    = qw(Exporter);
@EXPORT = qw();

# Begin-Doc
# Name: new
# Type: function
# Description: Creates new client object
# Syntax: $sync = new Local::SimpleRPC::Client(%opts)
# Comments: options are:
#    base_url: base url that rpc requests are issued against, the function name is appended to this URL
#    url_suffix: used if the target server requires an extension on the cgi files, such as ".pl" or ".exe"
#    authenticate: if true, will always pass auth info, will auto-set to 1 by default if URL contains 'auth-perl-bin',
#    user: optional, will auto-determine
#    password: optional, will auto-determine
#    debug: enable/disable debuging (1/0)
# End-Doc
sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;

    my $tmp = {};

    $tmp->{debug} = $opts{debug};
    $tmp->{error} = undef;

    $tmp->{base_url} = $opts{base_url}
        || croak "no base url provided, will not create object";

    $tmp->{url_suffix} = $opts{url_suffix};

    $tmp->{user} = $opts{user} || ( getpwuid($<) )[0];

    $tmp->{authenticate} = 0;
    if ( $tmp->{base_url} =~ /auth-perl-bin/ ) {
        $tmp->{authenticate} = 1;
    }
    if ( defined( $opts{authenticate} ) ) {
        $tmp->{authenticate} = $opts{authenticate};
    }

    $tmp->{password} = $opts{password};
    if ( $tmp->{authenticate} && !defined( $tmp->{password} ) ) {
        $tmp->{password}
            = &AuthSrv_Fetch( user  => $tmp->{user}, instance => "ads" )
            || &AuthSrv_Fetch( user => $tmp->{user}, instance => "afs" );
    }

    if ( $opts{authenticate} && !$tmp->{password} ) {
        croak "Authenticated API requested, but cannot determine password.\n";
    }

    # Append ::Stub, this object will have no methods other than underlying
    # RPC calls.
    return bless $tmp, $class . "::Stub";
}

# Server components - this will have
package Local::SimpleRPC::Server;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use Data::Dumper;
use Local::PrivSys;
use Local::HTMLUtil;
use JSON;
use Carp;

@ISA    = qw(Exporter);
@EXPORT = qw();

# Begin-Doc
# Name: new
# Type: function
# Description: Creates new server object.
# Syntax: $sync = new Local::SimpleRPC::Server(%opts)
# Comments: options are:
#    debug: enable/disable debuging (1/0)
# End-Doc
sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;

    my $tmp = {};

    $tmp->{debug} = $opts{debug};
    $tmp->{error} = undef;

    return bless $tmp, $class;
}

# Begin-Doc
# Name: Init
# Type: method
# Description: retrieves cgi request parms, and returns content type header
# Syntax: $obj->Init();
# End-Doc
sub Init {
    my $self = shift;

    &HTMLGetRequest();
    &HTMLContentType("application/json");

    return;
}

# Begin-Doc
# Name: Finish
# Type: method
# Description: outputs a standard formatted response with ok status and exits
# Syntax: $obj->Finish(@results);
# End-Doc
sub Finish {
    my $self = shift;

    print to_json( [ 0, "", @_ ] );
    exit(0);
}

# Begin-Doc
# Name: Fail
# Type: method
# Description: outputs a standard formatted response with failure status and exits
# Syntax: $obj->Fail($msg);
# End-Doc
sub Fail {
    my $self = shift;
    my $msg = shift || "Unknown Error";

    print to_json( [ 1, $msg ] );
    exit(0);
}

# Begin-Doc
# Name: RequirePriv
# Type: method
# Description: outputs a standard formatted response with failure status and exits
# Syntax: $obj->RequirePriv($privcode);
# End-Doc
sub RequirePriv {
    my $self = shift;
    my $code = shift;

    if ( &PrivSys_CheckPriv( $ENV{REMOTE_USER}, $code ) ) {
        return;
    }
    else {
        $self->Fail("Access Denied: RPC requires privilege ($code).");
    }
}

1;
