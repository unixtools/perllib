
=begin
Begin-Doc
Name: Local::AuthSrv
Type: module
Description: password stashing utilities for perl scripts
Requires: authorization in privsys to actually stash password, and host set up to support it
Required: use of AuthTools if you want to use the Store routines

Comment: Note - modules such as the oracle module typically default to using AuthSrv
behind the scenes, so you shouldn't usually have to use AuthSrv directly yourself.

Example: 
use Local::OracleObject;
use Local::AuthSrv;

$db = new Local::OracleObject;

$userid = "myuserid";
$passwd = &AuthSrv_Fetch(
                user => $userid,
                instance => "oracle");

$db->SQL_OpenDatabase("umr",
        user =&gt; $userid,
        passwd =&gt; $passwd) || $db->SQL_Error("open db");

End-Doc

=cut

package Local::AuthSrv;
require 5.000;
require Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw(
    AuthSrv_Fetch
    AuthSrv_Authenticate
    AuthSrv_Unauthenticate
);

$| = 1;

my $AUTHSRV_CACHE = {};

my $AUTHSRV_ENCRYPT   = "/usr/bin/authsrv-encrypt";
my $AUTHSRV_DECRYPT   = "/usr/bin/authsrv-decrypt";
my $AUTHSRV_DELETE    = "/usr/bin/authsrv-delete";

# Begin-Doc
# Name: AuthSrv_Fetch
# Type: function
# Description: fetch a stashed password
# Syntax: $pw = &AuthSrv_Fetch(instance => $instance, [user => $userid] );
# Comments: Returns stashed password. 'user' defaults to the
#       current userid on unix.
# End-Doc
sub AuthSrv_Fetch {
    my (%opts) = @_;
    my $instance = $opts{instance} || return undef;
    my $user     = $opts{user}     || ( getpwuid($<) )[0];
    my $passwd;

    if ( !defined( $AUTHSRV_CACHE->{$user}->{$instance} ) ) {
        open( AUTHSRV_SV_STDERR, ">&STDERR" );
        close(STDERR);

        open( AUTHSRV_FETCH_IN, "-|" )
            || exec( $AUTHSRV_DECRYPT, $user, $instance );
        while ( my $line = <AUTHSRV_FETCH_IN> ) {
            chomp($line);
            $passwd .= $line;
        }
        close(AUTHSRV_FETCH_IN);

        open( STDERR, ">&AUTHSRV_SV_STDERR" );

        $AUTHSRV_CACHE->{$user}->{$instance} = $passwd;
    }

    return $AUTHSRV_CACHE->{$user}->{$instance};
}

1;

