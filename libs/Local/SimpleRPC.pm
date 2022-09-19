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

our $AUTOLOAD;

BEGIN {
    &LogAPIUsage();
}

# Begin-Doc
# Name: AUTOLOAD
# Type: function
# Description: perl internal/magic AUTOLOAD function that implements the RPC call
# Syntax: Never called directly, perl calls when a RPC client issues $rpc->RPCMethod(@args)
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
    do {
        $debug
            && print "Passing to CallRPC($name, \@_) with $retries retries remaining.\n";
        eval { @results = $client->CallRPC( $name, @_ ); };
        if ($@) {
            push( @errors, $@ );
        }
        $retries--;
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
#    base_url: base url that rpc requests are issued against, the function name is appended to this URL
#    url_suffix: used if the target server requires an extension on the cgi files, such as ".pl" or ".exe"
#    authenticate: if true, will always pass auth info, will auto-set to 1 by default if URL contains 'auth-perl-bin',
#    user: optional, will auto-determine
#    password: optional, will auto-determine
#    retries: automatically retry on failure up to this many times, set to 0 to disable retries by default
#    timeout: set LWP client request timeout
#    allow_unsafe: allow unsafe operations such as authenticated requests on a non-https URL
#    debug: enable/disable debuging (1/0)
#    pre_args: array ref, args inserted at beginning of every rpc request issued by this object
#    post_args: array ref, args inserted at end of every rpc request issued by this object
# End-Doc
sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;

    my $tmp = {};

    &LogAPIUsage();

    $tmp->{debug} = $opts{debug};
    $tmp->{error} = undef;

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
# Returns: array of results returned in json response from the RPC
# End-Doc
sub CallRPC {
    my $self = shift;
    my $name = shift;
    my @args = @_;

    $self->{error} = undef;

    my $debug = $self->{debug};

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
    $debug && print "request: $req_content\n";
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

    $debug && print "response: $content\n";

    my $jsonret;
    eval { $jsonret = from_json( decode( 'UTF-8', $content ) ); };
    if ( !$jsonret ) {
        eval { $jsonret = from_json($content); };
    }

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
#    debug: enable/disable debuging (1/0)
#    pretty: enables/disable easy-to-read json output (1/0)
#    cgi: allow passing in cgi object such as when using with FastCGI loop
# End-Doc
sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;

    my $tmp = {};

    &LogAPIUsage();

    $tmp->{debug}  = $opts{debug};
    $tmp->{pretty} = $opts{pretty};
    $tmp->{error}  = undef;
    $tmp->{cgi}    = $opts{cgi};

    return bless $tmp, $class;
}

# Begin-Doc
# Name: Init
# Type: method
# Description: retrieves cgi request parms, and returns content type header
# Syntax: %rqpairs = $obj->Init();
# End-Doc
sub Init {
    my $self = shift;

    if ( $self->{cgi} ) {
        &HTMLSetCGI( $self->{cgi} );
    }
    &HTMLGetRequest();

    if ( $self->{pretty} ) {
        &HTMLContentType("text/plain");
    }
    else {
        &HTMLContentType("application/json");
    }

    $self->{cgi} = &HTMLGetCGI();

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
# Description: pass-thru to CGI module param method to retrieve parameters from cgi request
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

    return $cgi->param($name);
}

# Begin-Doc
# Name: multi_param
# Type: method
# Description: pass-thru to CGI module multi_param method to retrieve parameters from cgi request
# Syntax: @vals = $obj->multi_param("name");
# Syntax: $val = $obj->multi_param("name");
# End-Doc
sub multi_param {
    my $self = shift;
    my $name = shift;
    my $cgi  = $self->{cgi};

    # This will only work on newer CGI.pm's - specifically won't work on FC20
    return $cgi->multi_param($name);
}

# Begin-Doc
# Name: Try
# Type: method
# Description: attempts to execute a code block, terminates with json failure if eval fails
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
# Description: outputs a standard formatted response with ok status and exits
# Syntax: $obj->_json_out(@results);
# End-Doc
sub _json_print {
    my $self = shift;
    my $json = new JSON;

    my $js;
    if ( $self->{pretty} ) {
        $js = $json->pretty->encode(@_);
    }
    else {
        $js = $json->encode(@_);
    }

    # Potential for mixed/partially converted output, but won't die
    my $ejs = encode( 'UTF-8', $js, Encode::FB_QUIET );

    print $ejs;
}

# Begin-Doc
# Name: Finish
# Type: method
# Description: outputs a standard formatted response with ok status and exits
# Syntax: $obj->Finish(@results);
# End-Doc
sub Finish {
    my $self = shift;

    $self->_json_print( [ 0, "", @_ ] );
    exit(0);
}

# Begin-Doc
# Name: FinishReturn
# Type: method
# Description: outputs a standard formatted response with ok status and returns
# Syntax: $obj->FinishReturn(@results);
# End-Doc
sub FinishReturn {
    my $self = shift;

    $self->_json_print( [ 0, "", @_ ] );
    return (0);
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

    $self->_json_print( [ 1, $msg ] );
    exit(0);
}

# Begin-Doc
# Name: FailReturn
# Type: method
# Description: outputs a standard formatted response with failure status and returns non-zero
# Syntax: $obj->FailReturn($msg);
# End-Doc
sub FailReturn {
    my $self = shift;
    my $msg = shift || "Unknown Error";

    $self->_json_print( [ 1, $msg ] );
    return (1);
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
# Description: outputs a standard formatted response with failure status and returns nonzero if failed
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
