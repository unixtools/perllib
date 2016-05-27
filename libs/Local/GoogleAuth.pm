#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#

=begin
Begin-Doc
Name: Local::GoogleAuth
Type: module
Description: object that provides easy access to obtain a google web token
Comments: 

Setup procedure - common steps:

1. Establish account on the google domain that has sufficient admin privileges to perform the relevant operations
2. Go to https://console.developers.google.com/project
3. Create a Project if one isn't already created
4. Click on "APIs & Auth -> Credentials"
5. Update at least application name on Consent Screen tab.

Additional steps for using an "Installed Application" client ID - usable with normal APIs and expected to
interactively grant permission using 'authorize' api. 

1. Click "Credentials -> Add -> OAuth 2.0 Client ID"
2. Give the client a friendly name, and then click on the name, and 'Download JSON' to download a .json key file.
3. Import that file into authsrv: cat x.json | authsrv-raw-encrypt myuser myuser@example.com google-native-client-id
4. Delete any downloaded credentials files since they contain secure content
5. Click on "APIs & Auth -> APIs" and enable any/all of the APIs you might want to use
6. Use the authorize method in this module to create refresh token and authorize any desired API scopes.

Additional steps for using a "Service Account" client ID - usable with explicit grants of scopes in google admin
control panel, intended for user impersonation for data access:

1. Click "Create new Client ID -> Service Account -> JSON Key -> Create Client ID"
2. It will download a .json key file
3. Import that file into authsrv:  cat x.json | authsrv-raw-encrypt myuser myuser@example.com google-json-key
4. Delete any downloaded credentials files since they contain secure content
5. Go to google apps control panel, explicitly authorize any API scopes that are required.

Example Authorize Script:

my $ga = new UMR::GoogleAuth(
    user  => "user\@domain.com",
    email => "user\@domain.com",
);

$ga->authorize(
    scopes      => "https://www.googleapis.com/auth/admin.directory.user.readonly",
    incremental => 1
);

End-Doc

=cut

package Local::GoogleAuth;
use Exporter;
use strict;
use Local::UsageLogger;
use Local::CurrentUser;
use Local::AuthSrv;
use HTML::Entities;
use URI::Escape;
use LWP;
use JSON::WebToken;
use JSON;
use Sys::Hostname;

$| = 1;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw();

BEGIN {
    &LogAPIUsage();
}

# Begin-Doc
# Name: new
# Type: method
# Description: create a new GoogleAuth object
# Syntax: $obj = new GoogleAuth(%opts)
# Comments: debug => enable debug output if nonzero
# Comments: userid => userid for authsrv retrieval
# Comments: email => email for google login
# End-Doc
sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;

    &LogAPIUsage();

    my $tmp = {};
    $tmp->{debug} = $opts{debug} || 0;
    $tmp->{user} = $opts{user} || &Local_CurrentUser() || return undef;

    $tmp->{email} = $opts{email};
    if ( !$tmp->{email} ) {
        my $domain = hostname;
        $domain =~ s/^.*\.([^\.]+\.[^\.]+)$/$1/o;
        $tmp->{email} = $tmp->{user} . '@' . $domain;
    }

    return bless $tmp, $class;
}

# Begin-Doc
# Name: debug
# Type: method
# Access: public
# Description: Sets or returns current module debugging level
# Syntax: $obj->debug(1) to enable
# Syntax: $obj->debug(0) to disable
# End-Doc
sub debug {
    my $self = shift;
    if (@_) {
        $self->{debug} = shift;
    }
    else {
        return $self->{debug};
    }
}

# Begin-Doc
# Name: error
# Type: method
# Access: public
# Description: Sets or returns current object error status
# End-Doc
sub error {
    my $self = shift;
    if (@_) {
        $self->{error} = shift;
    }
    else {
        return $self->{error};
    }
}

# Begin-Doc
# Name: token
# Type: method
# Access: public
# Description: Retrieves token for a given scope
# Syntax: $token = $obj->token( ["sub" => $email], scope => $scope, scopes => [$scope1,$scope2,...], [expires => $tstamp] );
# Comments: If expires is not specified, defaults to 2 minutes. Either scope or scopes should be specified
#
# End-Doc
sub token {
    my $self    = shift;
    my %opts    = @_;
    my $user    = $self->{user};
    my $scope   = $opts{scope};
    my $scopes  = $opts{scopes};
    my $expires = $opts{expires};

    if ($scopes) {
        if ( ref($scopes) eq "ARRAY" ) {
            $scope = join( " ", @$scopes );
        }
        else {
            $scope = $scopes;
        }
    }

    $self->error(undef);

    if ( !$expires || int($expires) < time ) {
        $expires = time + 120;
    }

    my $json_key = &AuthSrv_Fetch( user => $user, instance => "google-json-key" );
    my $key_data;
    eval { $key_data = decode_json($json_key); };
    if ( !$key_data ) {
        $self->error("Failed decoding google api json key");
        return undef;
    }

    my $private_key = $key_data->{private_key};
    my $iss         = $key_data->{client_email};

    my @prnsub;
    if ( $opts{sub} ) {
        push( @prnsub, "sub" => $opts{sub} );
    }
    else {
        push( @prnsub, "prn" => $self->{email} );
    }
    my $jwt = JSON::WebToken->encode(
        {   iss   => $iss,
            scope => $scope,
            aud   => 'https://accounts.google.com/o/oauth2/token',
            exp   => $expires,
            iat   => time,
            @prnsub
        },
        $private_key,
        'RS256',
        { typ => 'JWT' }
    );

    my $ua       = LWP::UserAgent->new();
    my $response = $ua->post(
        'https://accounts.google.com/o/oauth2/token',
        {   grant_type => encode_entities('urn:ietf:params:oauth:grant-type:jwt-bearer'),
            assertion  => $jwt
        }
    );

    unless ( $response->is_success() ) {
        $self->error( "Failure requesting auth token: " . $response->code . "\n" . $response->content );
        return undef;
    }

    my $data = {};
    eval { $data = decode_json( $response->content ); };
    if ( !$data->{access_token} ) {
        $self->error("Unable to obtain access token");
        return undef;
    }

    return $data->{access_token};
}

# Begin-Doc
# Name: access_token_from_refresh_token
# Type: method
# Access: public
# Description: Retrieves access token from a refresh token
# Syntax: ($token,$expires) = $obj->access_token_from_refresh_token( [instance => "google-native-client-id"], [refreshinstance => "google-refresh-token"] );
#
# End-Doc
sub access_token_from_refresh_token {
    my $self             = shift;
    my %opts             = @_;
    my $user             = $self->{user};
    my $instance         = $opts{instance} || "google-native-client-id";
    my $refresh_instance = $opts{refreshinstance} || "google-refresh-token";

    $self->error(undef);

    my $json_key = &AuthSrv_Fetch( user => $user, instance => $instance );
    my $key_data;
    eval { $key_data = decode_json($json_key); };
    if ( !$key_data ) {
        $self->error("Failed decoding google api native client id key");
        return undef;
    }

    my $refresh_token = &AuthSrv_Fetch( user => $user, instance => $refresh_instance );

    my $id     = $key_data->{installed}->{client_id};
    my $secret = $key_data->{installed}->{client_secret};

    my @content_pieces;
    push( @content_pieces, "client_id=" . URI::Escape::uri_escape($id) );
    push( @content_pieces, "client_secret=" . URI::Escape::uri_escape($secret) );
    push( @content_pieces, "refresh_token=" . URI::Escape::uri_escape($refresh_token) );
    push( @content_pieces, "grant_type=refresh_token" );

    my $req_content = join( "&", @content_pieces );

    my $ua = LWP::UserAgent->new();
    my $req = HTTP::Request->new( POST => "https://www.googleapis.com/oauth2/v3/token" );
    $req->content_type("application/x-www-form-urlencoded");
    $req->content($req_content);

    my $response = $ua->request($req);

    unless ( $response->is_success() ) {
        $self->error( "Failure requesting auth token: " . $response->code . "\n" . $response->content );
        return undef;
    }

    my $data = {};
    eval { $data = decode_json( $response->content ); };
    if ( !$data->{access_token} ) {
        $self->error("Unable to obtain access token");
        return undef;
    }

    return ( $data->{access_token}, time + $data->{expires_in} );
}

# Begin-Doc
# Name: authorize
# Type: method
# Access: public
# Description: Retrieves refresh token and requests authorization for a given scope
# Syntax: $token = $obj->authorize( scope => $scope, scopes => [$scope1,$scope2,...], [incremental => 0], [instance => "google-native-client-id"], [refreshinstance => "google-refresh-token"] );
# Comments: If expires is not specified, defaults to 2 minutes. Either scope or scopes should be specified. Defaults to incrementally
# adding scopes to previously granted scopes for same client id.
#
# End-Doc
sub authorize {
    my $self                   = shift;
    my %opts                   = @_;
    my $user                   = $self->{user};
    my $scope                  = $opts{scope};
    my $scopes                 = $opts{scopes};
    my $instance               = $opts{instance} || "google-native-client-id";
    my $refresh_token_instance = $opts{refreshinstance} || "google-refresh-token";
    my $incremental            = 1;

    if ( defined( $opts{incremental} ) ) {
        $incremental = $opts{incremental};
    }

    if ($scopes) {
        if ( ref($scopes) eq "ARRAY" ) {
            $scope = join( " ", @$scopes );
        }
        else {
            $scope = $scopes;
        }
    }

    $self->error(undef);

    my $json_key = &AuthSrv_Fetch( user => $user, instance => $instance );
    my $key_data;
    eval { $key_data = decode_json($json_key); };
    if ( !$key_data ) {
        $self->error("Failed decoding google api json key");
        return undef;
    }

    my $id     = $key_data->{installed}->{client_id};
    my $secret = $key_data->{installed}->{client_secret};

    my $scopestring = URI::Escape::uri_escape($scope);

    my $url
        = "https://accounts.google.com/o/oauth2/auth?scope=${scopestring}&"
        . "redirect_uri=urn:ietf:wg:oauth:2.0:oob&"
        . "response_type=code&"
        . "client_id=${id}";

    if ($incremental) {
        $url .= "&include_granted_scopes=true";
    }

    print "\n\n";
    print "Open this URL in browser, authenticate with desired account and grant the requested access:\n";

    print "\n";

    print "$url\n";

    print "\n";

    print "Enter the code provided in the browser: ";
    my $code = <STDIN>;
    chomp($code);

    my @content_pieces;
    push( @content_pieces, "code=" . URI::Escape::uri_escape($code) );
    push( @content_pieces, "client_id=" . URI::Escape::uri_escape($id) );
    push( @content_pieces, "client_secret=" . URI::Escape::uri_escape($secret) );
    push( @content_pieces, "redirect_uri=oob" );
    push( @content_pieces, "grant_type=authorization_code" );

    my $req_content = join( "&", @content_pieces );

    my $ua = LWP::UserAgent->new();
    my $req = HTTP::Request->new( POST => "https://www.googleapis.com/oauth2/v3/token" );
    $req->content_type("application/x-www-form-urlencoded");
    $req->content($req_content);

    my $response = $ua->request($req);

    unless ( $response->is_success() ) {
        print "Failure requesting auth and refresh token: " . $response->code . "\n" . $response->content . "\n\n";
        return undef;
    }

    my $data = {};
    eval { $data = decode_json( $response->content ); };
    if ( !$data->{refresh_token} ) {
        print "Unable to obtain refresh token\n";
        return undef;
    }

    my $myuser = &Local_CurrentUser();
    open( my $out, "|-" ) || exec( "/usr/bin/authsrv-raw-encrypt", $myuser, $self->{user}, $refresh_token_instance );
    print $out $data->{refresh_token};
    close($out);

    print "Refresh token stored in owner $myuser, userid ", $self->{user}, ", instance $refresh_token_instance\n";

    return;
}

1;
