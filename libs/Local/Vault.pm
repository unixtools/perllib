#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
#

# Begin-Doc
# Name: Local::Vault
# Type: module
# Description: simplified vault client for Perl
# Comments: This has access to subnet configs, IP allocations, Ethernet address lookup, etc.
# End-Doc

package Local::Vault;
require Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

use Local::UsageLogger;
use Local::Encode;
use LWP;
use JSON;

@ISA    = qw(Exporter);
@EXPORT = qw();

BEGIN {
    &LogAPIUsage();
}

# Begin-Doc
# Name: new
# Type: function
# Description: Creates an object
# Syntax: $vault = new Local::Vault(%opts)
# Comments: optionally pass in user + password, role_id + secret_id, k8s_mount + k8s_token + k8s_role, or token
# End-Doc
sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;

    my $tmp = {};

    $tmp->{ua} = new LWP::UserAgent;
    $tmp->{ua}->timeout(5);
    my $ua = $tmp->{ua};

    &LogAPIUsage();

    # Default from environment if provided
    $tmp->{url} = $ENV{VAULT_ADDR};

    if ( $opts{url} ) {
        $tmp->{url} = $opts{url};
        $tmp->{url} =~ s|/$||go;
    }
    if ( $opts{user} ) {
        $tmp->{user} = $opts{user};
    }
    if ( $opts{password} ) {
        $tmp->{password} = $opts{password};
    }
    if ( $opts{token} ) {
        $tmp->{token} = $opts{token};
    }
    if ( $opts{role_id} ) {
        $tmp->{role_id} = $opts{role_id};
    }
    if ( $opts{secret_id} ) {
        $tmp->{secret_id} = $opts{secret_id};
    }
    if ( $opts{k8s_mount} ) {
        $tmp->{k8s_mount} = $opts{k8s_mount};
    }
    if ( $opts{k8s_token} ) {
        $tmp->{k8s_token} = $opts{k8s_token};
    }
    if ( $opts{k8s_role} ) {
        $tmp->{k8s_role} = $opts{k8s_role};
    }

    if ( !$tmp->{url} ) {
        die "must provide url";
    }

    if ( !$tmp->{token} ) {
        if ( $tmp->{user} && $tmp->{password} ) {
            my $req      = HTTP::Request->new( POST => $tmp->{url} . "/v1/auth/ldap/login/" . $tmp->{user} );
            my $authdata = { "password" => $tmp->{password} };
            $req->content( encode_json($authdata) );
            $req->content_type("application/json");

            my $resp = $ua->request($req);
            if ( $resp->is_success ) {
                my $info = decode_json( $resp->content );
                $tmp->{token} = $info->{auth}->{client_token};
            }
            else {
                die "Failed ldap auth.";
            }
        }
    }

    if ( !$tmp->{token} ) {
        if ( $tmp->{role_id} && $tmp->{secret_id} ) {
            my $req      = HTTP::Request->new( POST => $tmp->{url} . "/v1/auth/approle/login" );
            my $authdata = {
                "role_id"   => $tmp->{role_id},
                "secret_id" => $tmp->{secret_id}
            };
            $req->content( encode_json($authdata) );
            $req->content_type("application/json");

            my $resp = $ua->request($req);
            if ( $resp->is_success ) {
                my $info = decode_json( $resp->content );
                $tmp->{token} = $info->{auth}->{client_token};
            }
            else {
                die "Failed approle auth.";
            }
        }
    }

    if ( !$tmp->{token} ) {
        if ( $tmp->{k8s_mount} && $tmp->{k8s_token} && $tmp->{k8s_role} ) {
            my $req      = HTTP::Request->new( POST => $tmp->{url} . "/v1/auth/" . $tmp->{k8s_mount} . "/login" );
            my $authdata = {
                jwt  => $tmp->{k8s_token},
                role => $tmp->{k8s_role},
            };
            $req->content( encode_json($authdata) );
            $req->content_type("application/json");

            my $resp = $ua->request($req);
            if ( $resp->is_success ) {
                my $info = decode_json( $resp->content );
                $tmp->{token} = $info->{auth}->{client_token};
            }
            else {
                die "Failed k8s auth.";
            }
        }
    }

    if ( !$tmp->{token} ) {
        die "must provide means to get token or token itself";
    }

    return bless $tmp, $class;
}

# Begin-Doc
# Name: kv_get
# Type: function
# Description: Read a secret and return secret data
# Syntax: $secret = $vault->kv_get(path => "path/...", ["mount" => "secret"])
# Comments: The path should not include the mount prefix - default is 'secret'.
# End-Doc
sub kv_get {
    my $self = shift;
    my %opts = @_;

    if ( !$self->{token} ) { die; }
    my $ua    = $self->{ua};
    my $url   = $self->{url};
    my $token = $self->{token};

    my $mount = $opts{mount} || "secret";
    my $path  = $opts{path}  || die "must provide path";

    my $req = HTTP::Request->new( GET => "${url}/v1/${mount}/data/${path}" );
    $req->header( "X-Vault-Token" => $token );
    my $resp = $ua->request($req);
    if ( $resp->is_success ) {
        my $info = decode_json( $resp->content );
        return $info->{data}->{data};
    }
    else {
        die "error retrieving $path";
    }
}

# Begin-Doc
# Name: kv_get_full
# Type: function
# Description: Read a secret and return the entire vault response
# Syntax: $secret = $vault->kv_get_full(path => "path/...", ["mount" => "secret"])
# Comments: The path should not include the mount prefix - default is 'secret'.
# End-Doc
sub kv_get_full {
    my $self = shift;
    my %opts = @_;

    if ( !$self->{token} ) { die; }
    my $ua    = $self->{ua};
    my $url   = $self->{url};
    my $token = $self->{token};

    my $mount = $opts{mount} || "secret";
    my $path  = $opts{path}  || die "must provide path";

    my $req = HTTP::Request->new( GET => "${url}/v1/${mount}/data/${path}" );
    $req->header( "X-Vault-Token" => $token );
    my $resp = $ua->request($req);
    if ( $resp->is_success ) {
        my $info = decode_json( $resp->content );
        return $info;
    }
    else {
        die "error retrieving full $path";
    }
}

# Begin-Doc
# Name: read
# Type: function
# Description: Read a path and return the entire vault response
# Syntax: $secret = $vault->read(path => "path/...")
# End-Doc
sub read {
    my $self = shift;
    my %opts = @_;

    if ( !$self->{token} ) { die; }
    my $ua    = $self->{ua};
    my $url   = $self->{url};
    my $token = $self->{token};

    my $path = $opts{path} || die "must provide path";

    my $req = HTTP::Request->new( GET => "${url}/v1/${path}" );
    $req->header( "X-Vault-Token" => $token );
    my $resp = $ua->request($req);
    if ( $resp->is_success ) {
        my $info = decode_json( $resp->content );
        return $info;
    }
    else {
        die "error retrieving full $path";
    }
}

# Begin-Doc
# Name: list
# Type: function
# Description: Read a path returning the entire vault response. Note that path should
#  look like secret/metadata/folder1/foldern for the secret engine mounted at 'secret'
# Syntax: $secret = $vault->list(path => "path/...")
# End-Doc
sub list {
    my $self = shift;
    my %opts = @_;

    if ( !$self->{token} ) { die; }
    my $ua    = $self->{ua};
    my $url   = $self->{url};
    my $token = $self->{token};
    my $path  = $opts{path} || die "must provide path";

    my $req = HTTP::Request->new( LIST => "${url}/v1/${path}" );
    $req->header( "X-Vault-Token" => $token );
    my $resp = $ua->request($req);
    if ( $resp->is_success ) {
        my $info = decode_json( $resp->content );
        return $info;
    }
    else {
        die "error retrieving full $path";
    }
}

# Begin-Doc
# Name: write
# Type: function
# Description: Write to a path
# Syntax: $secret = $vault->write(path => "path/...", content => {});
# Comments: Path is the full path to root
# End-Doc
sub write {
    my $self = shift;
    my %opts = @_;

    if ( !$self->{token} ) { die; }
    my $ua    = $self->{ua};
    my $url   = $self->{url};
    my $token = $self->{token};

    my $content = $opts{content} || die "must provide content";
    my $path    = $opts{path}    || die "must provide path";

    my $req = HTTP::Request->new( POST => "${url}/v1/$path" );
    $req->content_type("application/json");
    $req->content( encode_json($content) );
    $req->header( "X-Vault-Token" => $token );

    my $resp = $ua->request($req);
    if ( $resp->is_success ) {
        return undef;
    }
    else {
        return $resp->content;
    }
}

1;
