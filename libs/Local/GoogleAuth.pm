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
Comments: must stash a google-json-key

Setup procedure:

1. Establish account on the google domain that has sufficient admin privileges to perform the relevant operations
2. Go to https://console.developers.google.com/project
3. Create a Project if one isn't already created
4. Click on "APIs & Auth -> Credentials"
5. Click "Create new Client ID -> Service Account -> Create Client ID -> Okay got it"
6. It will download a .p12 key file, delete this file you won't use it
7. Click "Generate new JSON key -> Okay got it"
8. Import that file into authsrv:  cat x.json | authsrv-raw-encrypt myuser myuser@example.com google-json-key
9. Delete the json file
10. Click on "APIs & Auth -> APIs" and enable any/all of the APIs you might want to use

End-Doc

=cut

package Local::GoogleAuth;
use Exporter;
use strict;
use Local::UsageLogger;
use Local::CurrentUser;
use Local::AuthSrv;
use HTML::Entities;
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
        $domain =~ s/^.*\.([^\.]+\.[^\.]+)$/\1/o;
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
# Syntax: $token = $obj->token( scope => $scope, scopes => [$scope1,$scope2,...], [expires => $tstamp] );
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

    my $jwt = JSON::WebToken->encode(
        {   iss   => $iss,
            scope => $scope,
            aud   => 'https://accounts.google.com/o/oauth2/token',
            prn   => $self->{email},
            exp   => $expires,
            iat   => time
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

1;
