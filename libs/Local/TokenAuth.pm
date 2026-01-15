#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
#

=begin
Begin-Doc
Name: Local::TokenAuth
Type: module
Description: simple api token authentication mechanism

Comments: 

Required Schema:

drop table if exists auth_clients;
create table auth_clients
(
    id integer not null primary key auto_increment,
    client_id varchar(50) not null,
    client_id_sub varchar(50),
    description varchar(250),
    client_secret_crypt varchar(200) not null,
    enabled enum('Y','N') default 'Y',
    last_auth datetime(3),
    auth_count integer not null default 0
);
create index ac_cila on auth_clients(client_id,last_auth);

End-Doc

=cut

package Local::TokenAuth;
use Exporter;
use strict;
use Carp;
use Sys::Hostname;
use MIME::Base64 qw(decode_base64);
use Digest::SHA qw(sha256_hex);
use JSON;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use Local::UsageLogger;

@ISA    = qw(Exporter);
@EXPORT = qw();

BEGIN {
    &LogAPIUsage();
}

=begin
Begin-Doc
Name: new
Type: method
Description: creates a new token auth object
Syntax: $obj = new Local::TokenAuth(db => $dbobject, [table => "auth_clients"])
Comments: Must be passed a database object handle
End-Doc
=cut

sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;
    my $tmp   = {};

    $tmp->{"db"} = $opts{db} || croak "Must specify database.";

    $tmp->{"table"} = $opts{table} || "auth_clients";

    $tmp->{"ttl_ok"}   = 300;
    $tmp->{"ttl_fail"} = 30;

    $tmp->{"cache"} = {};

    &LogAPIUsage();

    bless $tmp, $class;

    return $tmp;
}

=begin
Begin-Doc
Name: debug
Access: private
Syntax: $x = $obj->debug()
Syntax: $obj->debug($newval);
Type: method
End-Doc
=cut

sub debug {
    my $self = shift;

    if (@_) {
        $self->{debug} = shift;
    }
    else {
        return $self->{debug};
    }
}

=begin
Begin-Doc
Name: db
Access: private
Type: method
Comments: Returns db handle
Syntax: $x = $obj->db()
Syntax: $obj->db($newval);
End-Doc
=cut

sub db {
    my $self = shift;

    if (@_) {
        $self->{db} = shift;
    }
    else {
        return $self->{db};
    }
}

=begin
Begin-Doc
Name: cache
Access: private
Type: method
Comments: Returns cache hash reference
Syntax: $x = $obj->cache()
Syntax: $obj->cache()
End-Doc
=cut

sub cache {
    my $self = shift;
    return $self->{cache};
}

=begin
Begin-Doc
Name: is_authorized
Type: method
Description: checks authorization from environment headers and updates REMOTE_USER as needed
Syntax: $is_authorized = $obj->-is_authorized()
End-Doc
=cut

sub is_authorized {
    my $self  = shift;
    my $db    = $self->db();
    my $cache = $self->cache();

    my $auth          = $ENV{HTTP_AUTHORIZATION};
    my $client_id     = $ENV{HTTP_X_CLIENT_ID};
    my $client_secret = $ENV{HTTP_X_CLIENT_SECRET};

    # If basic auth provided, it takes precedence
    if ( $auth && $auth =~ /^Basic\s+(.*?)\s*$/ ) {
        my $decoded = decode_base64($1);

        ( $client_id, $client_secret ) = split( ':', $decoded, 2 );
    }

    if ( !$client_id ) {
        return 0;
    }
    if ( !$client_secret ) {
        return 0;
    }

    # Check in cache for previous auth
    my $hex_secret = sha256_hex($client_secret);
    my $cached     = $cache->{$client_id}->{$hex_secret};
    if ( $cached && $cached->{expires} && $cached->{expires} > time ) {
        if ( $cached->{status} ) {

            # All primary client ids are tied to userids
            $ENV{REMOTE_USER} = lc($client_id);

            # Optional secondary ID if the app wants to do permission limiting tokens
            if ( $cached->{client_id_sub} ) {
                $ENV{REMOTE_USER_SUB} = lc( $cached->{client_id_sub} );
            }
        }

        return $cached->{status};
    }

    # Nothing in cache, fall back to actually checking
    my $qry = qq{
select id, client_secret_crypt, client_id_sub
from auth_clients
where client_id=? and enabled='Y'
order by last_auth desc
};
    my $cid = $db->SQL_OpenQuery( $qry, $client_id );
    while ( my ( $id, $crypted, $client_id_sub ) = $db->SQL_FetchRow($cid) ) {
        if ( $crypted && crypt( $client_secret, $crypted ) eq $crypted ) {
            $db->SQL_ExecQuery( "update auth_clients set last_auth=now(3),auth_count=auth_count+1 where id=?", $id );

            # All primary client ids are tied to userids
            $ENV{REMOTE_USER} = lc($client_id);

            # Optional secondary ID if the app wants to do permission limiting tokens
            if ($client_id_sub) {
                $ENV{REMOTE_USER_SUB} = lc($client_id_sub);
            }
            $db->SQL_CloseQuery($cid);

            $cache->{$client_id}->{$hex_secret}
                = { expires => time + $self->{ttl_ok}, status => 1, client_id_sub => $client_id_sub };
            return 1;
        }
    }
    $db->SQL_CloseQuery($cid);

    # Return not authorized by default
    $cache->{client_id}->{$hex_secret} = { expires => time + $self->{ttl_fail}, status => 0 };
    return 0;
}

1;

