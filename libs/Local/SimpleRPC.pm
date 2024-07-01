#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
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
require Exporter;
use strict;
use Local::UsageLogger;

# Stub routines - autoloader only, used for easy referencing of remote routines
package Local::SimpleRPC::Client::Stub;
use Carp;
use strict;
use Local::UsageLogger;
use Time::HiRes qw(sleep);

our $AUTOLOAD;

BEGIN {
    &LogAPIUsage();
}

# Begin-Doc
# Name: AUTOLOAD
# Type: function
# Description: Perl internal/magic AUTOLOAD function that implements the RPC call
# Syntax: Never called directly, Perl calls when a RPC client issues $rpc->RPCMethod(@args)
# Comments: This routine calls back to the parent implementation object CallRPC routine that
# Comments: implements the RPC encoding of request, error checking, and response to caller.
# End-Doc
sub AUTOLOAD {
    my $self = shift;
    my $type = ref($self)
        or croak "$self is not an object";

    my $client = $self->{client};

    my $name = $AUTOLOAD;

    $name =~ s/.*://;    # strip fully-qualified portion
    return if ( $name eq "DESTROY" );

    my $debug = $self->{debug};

    $debug && print "AUTOLOAD called with $self / $type / $name\n";

    my @errors  = ();
    my @results = ();
    my $retries = $self->{retries};
    my $attempt = 0;
    do {
        $attempt++;
        $debug
            && print "Passing to CallRPC($name, \@_) with $retries retries remaining.\n";
        eval { @results = $client->CallRPC( $name, @_ ); };
        if ($@) {
            push( @errors, $@ );
        }
        $retries--;
        if ( $retries >= 0 && $attempt > 0 ) {
            # Delay 200ms, exponentially increasing up to 10 seconds max
            my $delay = 0.1 * (2 ** $attempt);
            if ( $delay > 10 ) { $delay = 10; }
            sleep($delay);
        }
    } while ( $@ && $retries >= 0 );    # retry up to $retries times, set to 0 for only a single request
    if ($@) {
        croak join( "\nFailed on retry: ", @errors );
    }

    if ( !wantarray && scalar(@results) == 1 ) {
        return $results[0];
    }
    else {
        return @results;
    }
}

# Client component
package Local::SimpleRPC::Client;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use Data::Dumper;
use Local::AuthSrv;
use Local::CurrentUser;
use Local::UsageLogger;
use Carp;
use LWP::UserAgent;
use URI::Escape;
use Encode qw(decode);
use JSON;
use strict;

@ISA    = qw(Exporter);
@EXPORT = qw();

BEGIN {
    &LogAPIUsage();
}

# Begin-Doc
# Name: new
# Type: function
# Description: Creates new client object
# Syntax: $sync = new Local::SimpleRPC::Client(%opts)
# Comments: options are:
#    version: version of SimpleRPC request/response format, v2 switches to HTTP codes and native return from functions
#    base_url: base URL that RPC requests are issued against, the function name is appended to this URL
#    url_suffix: used if the target server requires an extension on the CGI files, such as ".pl" or ".exe"
#    authenticate: if true, will always pass auth info, will auto-set to 1 by default if URL contains 'auth-perl-bin',
#    user: optional, will auto-determine
#    password: optional, will auto-determine
#    retries: automatically retry on failure up to this many times, set to 0 to disable retries by default
#    timeout: set LWP client request timeout
#    allow_unsafe: allow unsafe operations such as authenticated requests on a non-HTTPS URL
#    debug: enable/disable debuging (1/0)
#    pre_args: array ref, args inserted at beginning of every RPC request issued by this object
#    post_args: array ref, args inserted at end of every RPC request issued by this object
# End-Doc
sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;

    my $tmp = {};

    &LogAPIUsage();

    $tmp->{debug} = $opts{debug};
    $tmp->{error} = undef;

    $tmp->{version} = $opts{version} || 1;

    $tmp->{base_url} = $opts{base_url}
        || croak "no base url provided, will not create object";

    $tmp->{url_suffix} = $opts{url_suffix};

    $tmp->{user}    = $opts{user};
    $tmp->{retries} = int( $opts{retries} );
    $tmp->{timeout} = int( $opts{timeout} ) || 30;

    $tmp->{allow_unsafe} = int( $opts{allow_unsafe} );

    $tmp->{authenticate} = 0;
    if ( $tmp->{base_url} =~ m|/auth-perl-bin| ) {
        $tmp->{authenticate} = 1;
    }
    elsif ( $tmp->{base_url} =~ m|/auth-cgi-bin| ) {
        $tmp->{authenticate} = 1;
    }
    elsif ( $tmp->{base_url} =~ m|/auth-fcgi-bin| ) {
        $tmp->{authenticate} = 1;
    }
    elsif ( $tmp->{base_url} =~ m|/auth-api-bin| ) {
        $tmp->{authenticate} = 1;
    }
    elsif ( $tmp->{base_url} =~ m|/auth-cgi| ) {
        $tmp->{authenticate} = 1;
    }
    elsif ( $tmp->{base_url} =~ m|/auth-cgid| ) {
        $tmp->{authenticate} = 1;
    }

    if ( defined( $opts{authenticate} ) ) {
        $tmp->{authenticate} = $opts{authenticate};
    }

    if ( $tmp->{authenticate} && !$tmp->{user} ) {
        $tmp->{user} = &Local_CurrentUser();
    }

    if (   $tmp->{authenticate}
        && $tmp->{base_url} !~ m|^https://|o
        && !$tmp->{allow_unsafe} )
    {
        croak "will not allow authenticated request on not https:// url unless allow_unsafe is set!";
    }

    $tmp->{password} = $opts{password};
    if ( $tmp->{authenticate} && !defined( $tmp->{password} ) ) {
        $tmp->{password} = &AuthSrv_Fetch( user => $tmp->{user}, instance => "ads" );
    }

    if ( $opts{authenticate} && !$tmp->{password} ) {
        croak "Authenticated API requested, but cannot determine password.\n";
    }

    if ( $tmp->{pre_args} && ref( $tmp->{pre_args} ) ne "ARRAY" ) {
        croak "pre_args parameter must be array reference if specified.\n";
    }
    $tmp->{pre_args} = $opts{pre_args};

    if ( $tmp->{post_args} && ref( $tmp->{post_args} ) ne "ARRAY" ) {
        croak "post_args parameter must be array reference if specified.\n";
    }
    $tmp->{post_args} = $opts{post_args};

    # Return ref to self so we can reuse for retries/etc.
    my $clientref = {};
    %$clientref = %$tmp;
    bless $clientref, $class;

    $tmp->{client} = $clientref;

    # Append ::Stub, this object will have no methods other than underlying
    # RPC calls.
    return bless $tmp, $class . "::Stub";
}

# Begin-Doc
# Name: CallRPC
# Description: driver/worker routine that implements the RPC operation
# Syntax: $obj->RPCName(@args);
# Returns: array of results returned in JSON response from the RPC
# End-Doc
sub CallRPC {
    my $self = shift;
    my $name = shift;
    my @args = @_;

    $self->{error} = undef;

    my $debug   = $self->{debug};
    my $version = $self->{version};

    $debug && print "CallRPC called with $self / $name\n";

    my $url = $self->{base_url} || croak "no base url provided";
    $url =~ s|/*$||go;
    $url .= "/" . $name;

    if ( $self->{url_suffix} ) {
        $url .= $self->{url_suffix};
    }

    $debug && print "Submit to URL: $url\n";

    if ( !$self->{ua} ) {
        my $ua = new LWP::UserAgent;
        $ua->timeout( $self->{timeout} );
        $ua->agent("SimpleRPC/1.0");
        $self->{ua} = $ua;

        # Do not attempt to use $ua->conn_cache, while it should make things faster
        # there is something wrong that results in it slowing things down a LOT.
    }

    my $req = HTTP::Request->new( POST => $url );
    if ( $self->{user} && $self->{password} ) {
        $debug && print "submitting with user and password\n";
        $req->authorization_basic( $self->{user}, $self->{password} );
    }
    $req->content_type("application/x-www-form-urlencoded");

    # Standard is that args is a 'hash in array form'
    my @content_pieces;
    my @all_args;
    if ( ref( $self->{pre_args} ) eq "ARRAY" ) {
        push( @all_args, @{ $self->{pre_args} } );
    }
    push( @all_args, @args );
    if ( ref( $self->{post_args} ) eq "ARRAY" ) {
        push( @all_args, @{ $self->{post_args} } );
    }
    while ( scalar(@all_args) ) {
        my $a = shift @all_args;
        if ( scalar(@all_args) > 0 ) {
            my $b = shift @all_args;
            push( @content_pieces, URI::Escape::uri_escape($a) . "=" . URI::Escape::uri_escape($b) );
        }
        else {
            push( @content_pieces, URI::Escape::uri_escape($a) );
        }
    }

    # Fill with request parms first
    my $req_content = join( "&", @content_pieces );
    if ($req_content) {
        $debug && print "request: $req_content\n";
    }
    else {
        $debug && print "request: no parameters\n";
    }
    $req->content($req_content);

    my $ua = $self->{ua};
    my $res;
    eval {
        # Exit will be ugly, but will at least function as a timeout
        local $SIG{ALRM} = sub { die "Timeout in SimpleRPC\n" };
        alarm( $self->{timeout} );
        $res = $ua->request($req);
        alarm(0);
    };

    if ( !$res ) {
        $self->{error} = "Error performing LWP request.";
        croak $self->{error};
    }

    # If older version and we get a non-successful response code, instant failure, newer versions use
    # variable http response codes for status.
    if ( $version < 2 ) {
        if ( !$res->is_success ) {
            $self->{error} = "LWP Request Failed: " . $res->message;
            croak $self->{error};
        }
    }

    # New apps will always try to use json return for structure and will use http error codes
    # So we will get "non-success" responses
    my $content = $res->content;
    if ( !$content ) {
        $self->{error} = "No content returned from LWP request.";
        croak $self->{error};
    }

    $debug && print "response: $content\n";

    my $jsonret;
    eval { $jsonret = from_json( decode( 'UTF-8', $content ) ); };
    if ( !$jsonret ) {
        eval { $jsonret = from_json($content); };
    }

    if ($@) {
        $self->{error} = "JSON Response Parsing Failed: " . $@;
        croak $self->{error};
    }

    # If we've been told we're version 2 or we get a HASH in data we know it is a version 2 server
    if ( $version > 1 || ref($jsonret) eq "HASH" ) {
        if ( !$res->is_success && ref($jsonret) eq "HASH" ) {
            $self->{error} = "API Request Returned Failure: " . $jsonret->{error};
            croak $self->{error};
        }
        elsif ( !$res->is_success ) {
            $self->{error} = "API Request Returned Unknown Failure: " . $res->content;
            croak $self->{error};
        }

        if ( ref($jsonret) ne "HASH" ) {
            $self->{error} = "Invalid response, not a JSON hash ($jsonret)";
            croak $self->{error};
        }

        if ( $jsonret->{data} ) {
            return @{ $jsonret->{data} };
        }
        else {
            return ();
        }
    }
    else {
        # Since we always get an array in v1, a null/false response is invalid
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

    $self->{error} = "Error in API: Should not get here";
    croak $self->{error};
}

# Server components - this has several utility routines for making the server side
# of the RPC very easy to implement in a small amount of code.
package Local::SimpleRPC::Server;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use Data::Dumper;
use Local::PrivSys;
use Local::HTMLUtil;
use Local::UsageLogger;
use JSON;
use Carp;
use Encode qw(encode);
use strict;

@ISA    = qw(Exporter);
@EXPORT = qw();

BEGIN {
    &LogAPIUsage();
}

# Begin-Doc
# Name: new
# Type: function
# Description: Creates new server object.
# Syntax: $sync = new Local::SimpleRPC::Server(%opts)
# Comments: options are:
#    debug: enable/disable debugging (1/0)
#    cgi: allow passing in CGI object such as when using with FastCGI loop
#    version: use newer return model that is more REST-like output format and uses HTTP status codes
# End-Doc
sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;

    my $tmp = {};

    # Accomodate invocations through FastCGI or other persistent server scripts
    &ResetAPIUsage();

    &LogAPIUsage();

    $tmp->{debug} = $opts{debug};
    $tmp->{error} = undef;
    $tmp->{cgi}   = $opts{cgi};

    $tmp->{version} = int( $opts{version} ) || 1;

    return bless $tmp, $class;
}

# Begin-Doc
# Name: Init
# Type: method
# Description: retrieves CGI request parms, and returns a content-type header
# Syntax: %rqpairs = $obj->Init();
# End-Doc
sub Init {
    my $self = shift;

    if ( $self->{cgi} ) {
        &HTMLSetCGI( $self->{cgi} );
    }

    &HTMLGetRequest();

    # Do not output content type here, will be handled in the returns to allow setting status header
    $self->{cgi} = &HTMLGetCGI();

    # Pull in json from posted data if possible and use it for param retrieval
    if ( $ENV{CONTENT_TYPE} eq "application/json" || $ENV{CONTENT_TYPE} eq "text/json" ) {
        my $raw = $self->{cgi}->param("POSTDATA");
        if ($raw) {
            eval { $self->{posted} = decode_json($raw); };
            if ($@) {
                $self->Fail("Could not decode posted JSON: $@");
            }
        }
    }

    # If we're running under mod_perl, to keep semantics, export into callers namespace as well
    my ($pkg) = caller(0);
    if ( $pkg =~ /^ModPerl/ ) {
        no strict "refs";
        *{ $pkg . "::rqpairs" } = *main::rqpairs;
    }

    # Preferred access path, above is for ease of migration
    return %main::rqpairs;
}

# Begin-Doc
# Name: param
# Type: method
# Description: pass-thru to CGI module param method to retrieve parameters from CGI request
# Syntax: @vals = $obj->param("name");
# Syntax: $val = $obj->param("name");
# End-Doc
sub param {
    my $self = shift;
    my $name = shift;
    my $cgi  = $self->{cgi};

    # Should review callers/usage, but this will silence warning for now since
    # the vulernability is due to poor code. Probably should split this into
    # two methods and do an eval check to try to use multi_param instead with
    # newer CGI.pm
    $CGI::LIST_CONTEXT_WARN = 0;

    if ( $self->{posted} && exists( $self->{posted}->{$name} ) ) {
        return $self->{posted}->{$name};
    }
    else {
        my $tmp = $cgi->param($name);
        if ( defined($tmp) ) {
            return $cgi->param($name);
        }
        else {
            return $cgi->url_param($name);
        }
    }
}

# Begin-Doc
# Name: multi_param
# Type: method
# Description: pass-thru to CGI module multi_param method to retrieve parameters from CGI request
# Syntax: @vals = $obj->multi_param("name");
# Syntax: $val = $obj->multi_param("name");
# End-Doc
sub multi_param {
    my $self = shift;
    my $name = shift;
    my $cgi  = $self->{cgi};

    # This will only work on newer CGI.pm's - specifically won't work on FC20
    if ( $self->{posted} && exists( $self->{posted}->{$name} ) ) {
        if ( ref( $self->{posted}->{$name} ) eq "ARRAY" ) {
            return @{ $self->{posted}->{$name} };
        }
        else {
            return ( $self->{posted}->{$name} );
        }
    }
    else {
        my @tmp = $cgi->multi_param($name);
        if ( scalar(@tmp) > 0 ) {
            return @tmp;
        }
        else {
            return $cgi->url_param($name);
        }
    }
}

# Begin-Doc
# Name: Try
# Type: method
# Description: attempts to execute a code block, terminates with JSON failure if eval fails
# Syntax: @res = $obj->Try($coderef);
# Syntax: @res = $obj->Try(sub { code here });
# Syntax: @res = $obj->Try(\&sub, arg1, arg2, ...);
# Comments: returns results of subroutine/coderef
# End-Doc
sub Try {
    my $self = shift;
    my $code = shift;
    my @res;

    # The eval will inherit @_ allowing for passing of arguments
    eval { @res = &$code; };
    if ($@) {
        $self->Fail($@);
    }

    if ( !wantarray && scalar(@res) == 1 ) {
        return $res[0];
    }
    else {
        return @res;
    }
}

# Begin-Doc
# Name: _json_out
# Type: method
# Description: outputs a standard formatted response with an ok status and exits
# Syntax: $obj->_json_out(@results);
# End-Doc
sub _json_print {
    my $self = shift;
    my $json = new JSON;

    my $js = $json->canonical->encode(@_);

    # Potential for mixed/partially converted output, but won't die
    my $ejs = encode( 'UTF-8', $js, Encode::FB_QUIET );

    print $ejs;
}

# Begin-Doc
# Name: FinishStatus
# Type: method
# Description: outputs a standard formatted response with an ok status and exits
# Syntax: $obj->FinishStatus($http_status_code, @results);
# End-Doc
sub FinishStatus {
    my $self   = shift;
    my $status = shift;

    if ( $self->{version} > 1 ) {
        print "Status: $status\n";
        &HTMLContentType("application/json");

        $self->_json_print( { data => [@_], status => "success", error => undef } );
    }
    else {
        print "Status: $status\n";
        &HTMLContentType("application/json");

        $self->_json_print( [ 0, "", @_ ] );
    }
    exit(0);
}

# Begin-Doc
# Name: FinishStatusReturn
# Type: method
# Description: outputs a standard formatted response with an ok status and return
# Syntax: $obj->FinishStatusReturn($http_status_code, @results);
# End-Doc
sub FinishStatusReturn {
    my $self   = shift;
    my $status = shift;

    if ( $self->{version} > 1 ) {
        print "Status: $status\n";
        &HTMLContentType("application/json");

        $self->_json_print( { data => [@_], status => "success", error => undef } );
    }
    else {
        print "Status: $status\n";
        &HTMLContentType("application/json");

        $self->_json_print( [ 0, "", @_ ] );
    }
    return (0);
}

# Begin-Doc
# Name: Finish
# Type: method
# Description: outputs a standard formatted response with an ok status and exits
# Syntax: $obj->Finish(@results);
# End-Doc
sub Finish {
    my $self = shift;

    if ( $self->{version} > 1 ) {
        print "Status: 200\n";
        &HTMLContentType("application/json");

        $self->_json_print( { data => [@_], status => "success", error => undef } );
    }
    else {
        print "Status: 200\n";
        &HTMLContentType("application/json");

        $self->_json_print( [ 0, "", @_ ] );
    }
    exit(0);
}

# Begin-Doc
# Name: FinishReturn
# Type: method
# Description: outputs a standard formatted response with an ok status and return
# Syntax: $obj->FinishReturn(@results);
# End-Doc
sub FinishReturn {
    my $self = shift;

    if ( $self->{version} > 1 ) {
        print "Status: 200\n";
        &HTMLContentType("application/json");

        $self->_json_print( { data => [@_], status => "success", error => undef } );
    }
    else {
        print "Status: 200\n";
        &HTMLContentType("application/json");

        $self->_json_print( [ 0, "", @_ ] );
    }
    return (0);
}

# Begin-Doc
# Name: Fail
# Type: method
# Description: outputs a standard formatted response with a failure status and exits
# Syntax: $obj->Fail($msg);
# End-Doc
sub Fail {
    my $self = shift;
    my $msg  = shift || "Unknown Error";

    if ( $self->{version} > 1 ) {
        print "Status: 400\n";
        &HTMLContentType("application/json");

        $self->_json_print( { status => "error", error => $msg } );
    }
    else {
        print "Status: 400\n";
        &HTMLContentType("application/json");

        $self->_json_print( [ 1, $msg, @_ ] );
    }
    exit(0);
}

# Begin-Doc
# Name: FailReturn
# Type: method
# Description: outputs a standard formatted response with a failure status and returns non-zero
# Syntax: $obj->FailReturn($msg);
# End-Doc
sub FailReturn {
    my $self = shift;
    my $msg  = shift || "Unknown Error";

    if ( $self->{version} > 1 ) {
        print "Status: 400\n";
        &HTMLContentType("application/json");

        $self->_json_print( { status => "error", error => $msg } );
    }
    else {
        print "Status: 400\n";
        &HTMLContentType("application/json");

        $self->_json_print( [ 1, $msg, @_ ] );
    }
    return (1);
}

# Begin-Doc
# Name: RequirePriv
# Type: method
# Description: outputs a standard formatted response with a failure status and exits
# Syntax: $obj->RequirePriv($privcode);
# End-Doc
sub RequirePriv {
    my $self = shift;
    my $code = shift;

    if ( eval { &PrivSys_CheckPriv( $ENV{REMOTE_USER}, $code ) } ) {
        return;
    }
    else {
        if ($@) {
            $self->Fail("Access Denied: An error occurred while attempting to verify RPC privilege ($code): $@");
        }
        else {
            $self->Fail("Access Denied: RPC requires privilege ($code).");
        }
    }
}

# Begin-Doc
# Name: RequirePrivReturn
# Type: method
# Description: outputs a standard formatted response with a failure status and returns nonzero if failed
# Syntax: $obj->RequirePrivReturn($privcode);
# End-Doc
sub RequirePrivReturn {
    my $self = shift;
    my $code = shift;

    if ( eval { &PrivSys_CheckPriv( $ENV{REMOTE_USER}, $code ) } ) {
        return 0;
    }
    else {
        if ($@) {
            $self->FailReturn("Access Denied: An error occurred while attempting to verify RPC privilege ($code): $@");
            return 1;
        }
        else {
            $self->FailReturn("Access Denied: RPC requires privilege ($code).");
            return 1;
        }
    }
}

# Begin-Doc
# Name: RequireAnyPriv
# Type: method
# Description: wrapper routine around privsys privilege check, require at least one of the listed privileges
# Syntax: $obj->RequireAnyPriv($code, [$code2, ...]);
# Comments: at least one code must be specified
# End-Doc
sub RequireAnyPriv {
    my $self  = shift;
    my @codes = @_;

    foreach my $code (@codes) {
        if ( eval { &PrivSys_CheckPriv( $ENV{REMOTE_USER}, $code ) } ) {
            return;
        }
        elsif ($@) {
            $self->Fail("Access Denied: An error occurred while attempting to verify RPC privilege ($code): $@");
        }
    }

    $self->Fail( "Access Denied: RPC required at least one of these priv codes: " . join( ", ", @codes ) );
}

# Begin-Doc
# Name: RequireAnyPrivReturn
# Type: method
# Description: wrapper routine around privsys privilege check, require at least one of the listed privileges, returns non-zero if failed
# Syntax: $obj->RequireAnyPrivReturn($code, [$code2, ...]);
# Comments: at least one code must be specified
# End-Doc
sub RequireAnyPrivReturn {
    my $self  = shift;
    my @codes = @_;

    foreach my $code (@codes) {
        if ( eval { &PrivSys_CheckPriv( $ENV{REMOTE_USER}, $code ) } ) {
            return 0;
        }
        elsif ($@) {
            $self->FailReturn("Access Denied: An error occurred while attempting to verify RPC privilege ($code): $@");
            return 1;
        }
    }

    $self->FailReturn( "Access Denied: RPC required at least one of these priv codes: " . join( ", ", @codes ) );
    return 1;
}

# Begin-Doc
# Name: RequireAllPrivs
# Type: method
# Description: wrapper routine around privsys privilege check, require all of the listed privileges
# Syntax: $obj->RequireAllPrivs($code, [$code2, ...]);
# Comments: at least one code must be specified
# End-Doc
sub RequireAllPrivs {
    my $self  = shift;
    my @codes = @_;

    if ( !@codes ) {
        $self->Fail("Access Denied: No code specified for RequireAllPrivs");
    }

    foreach my $code (@codes) {
        if ( eval { !&PrivSys_CheckPriv( $ENV{REMOTE_USER}, $code ) } ) {
            $self->Fail( "Access Denied: RPC required all of these priv codes: " . join( ", ", @codes ) );
        }
        elsif ($@) {
            $self->Fail("Access Denied: An error occurred while attempting to verify RPC privilege ($code): $@");
        }
    }

    return;
}

# Begin-Doc
# Name: RequireAllPrivsReturn
# Type: method
# Description: wrapper routine around privsys privilege check, require all of the listed privileges, returns non-zero on failure
# Syntax: $obj->RequireAllPrivsReturn($code, [$code2, ...]);
# Comments: at least one code must be specified
# End-Doc
sub RequireAllPrivsReturn {
    my $self  = shift;
    my @codes = @_;

    foreach my $code (@codes) {
        if ( eval { !&PrivSys_CheckPriv( $ENV{REMOTE_USER}, $code ) } ) {
            $self->FailReturn( "Access Denied: RPC required all of these priv codes: " . join( ", ", @codes ) );
            return 1;
        }
        elsif ($@) {
            $self->FailReturn("Access Denied: An error occurred while attempting to verify RPC privilege ($code): $@");
            return 1;
        }
    }

    if ( !@codes ) {
        $self->FailReturn("Access Denied: No code specified for RequireAllPrivs");
        return 1;
    }

    return 0;
}

1;
