
=begin
Begin-Doc
Name: Local::AuthSrv
Type: module
Description: password stashing utilities for perl scripts
Requires: authorization in privsys to actually stash password, and host set up to support it

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

$db->SQL_OpenDatabase("srvp",
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

my $AUTHSRV_ENCRYPT   = "authsrv-encrypt";
my $AUTHSRV_DECRYPT   = "authsrv-decrypt";
my $AUTHSRV_DELETE    = "authsrv-delete";
my $AUTHSRV_AUTH      = "authsrv-auth";
my $AUTHSRV_AUTH_EXEC = "authsrv-exec";

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
        if ( -e $AUTHSRV_DECRYPT || -e "/usr/bin/$AUTHSRV_DECRYPT" ) {
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
        }

        #
        # This is an ugly stop-gap, but allows for code transparency
        # between linux and windows. And it's still better than passwords
        # in the script itself.
        #
        elsif ( -e "C:\\Windows\\authsrv.dat" ) {
            my $line;
            open( AUTHSRV_FETCH_IN, "C:\\Windows\\authsrv.dat" );
            my $adata = join( "", <AUTHSRV_FETCH_IN> );
            close(AUTHSRV_FETCH_IN);
            foreach my $line ( split( /[\r\n]+/, $adata ) ) {
                my ( $file_user, $file_instance, $file_pw ) =
                  split( ' ', $line, 4 );

                if ( $file_user eq $user && $file_instance eq $instance ) {
                    $passwd = $file_pw;
                    last;
                }
            }
            close(AUTHSRV_FETCH_IN);
        }

        $AUTHSRV_CACHE->{$user}->{$instance} = $passwd;
    }

    return $AUTHSRV_CACHE->{$user}->{$instance};
}

# Begin-Doc
# Name: AuthSrv_Authenticate
# Type: function
# Description: authenticates current process into afs using user $user, instance afs
# Syntax: &AuthSrv_Authenticate(user => $user, keep_ccache => 0/1, use_existing_ccache => 0/1);
# Comments: $user is optional, will use uid of script owner if not.
#
# Note - if you are trying to use AuthSrv from a non-perl script, issue something equivalent
# to: system("/usr/bin/authsrv-auth yourafsuserid");
#
# If keep_ccache is non-zero, will attempt to allocate a kerberos credentials cache and keep
# it around. This is only useful if the script will be using kerberized command line apps
# such as ssh/scp. This option should not normally be needed.
#
# If use_existing_ccache is non-zero, will attempt to use an already existing ccache. This is
# probably only of use for systems programming apps, and then not very likely. If you are logged
# in with a telnet session and use_existing_ccache is non-zero, it will replace your current
# ccache with the newly authenticated one. use_existing_ccache implies keep_ccache. This
# option should not normally be needed.
#
# Your script should probably call the &AuthSrv_Unauthenticate() routine when it is finished
# so it will clean up your token and any existing credentials cache. This is especially important
# if you are using the keep_ccache or use_existing_ccache options.
#
# NOTE: This routine should basically be considered deprecated and not needed for anything any more.
#
# Returns: non-zero on failure
# End-Doc
sub AuthSrv_Authenticate {
    my (%opts) = @_;
    my $user                = $opts{user}                || ( getpwuid($<) )[0];
    my $keep_ccache         = $opts{keep_ccache}         || 0;
    my $use_existing_ccache = $opts{use_existing_ccache} || 0;

    &LogAPIUsage();

    if ( !$use_existing_ccache ) {
        $ENV{KRB5CCNAME} =
          "FILE:/tmp/krb5cc_authsrv_u" . $< . "_p" . $$ . "_" . time;
    }
    elsif ($use_existing_ccache) {
        if ( !defined( $ENV{KRB5CCNAME} ) ) {
            return 1;
        }
    }

    system( $AUTHSRV_AUTH, $user );

    if ( !$keep_ccache && !$use_existing_ccache ) {
        system("kdestroy >/dev/null 2>&1");
    }
}

# Begin-Doc
# Name: AuthSrv_Unauthenticate
# Type: function
# Description: clears afs authentication and removes credentials cache
# Syntax: &AuthSrv_Unauthenticate();
# Comments: Should be run at end of script (or when done with afs) whenever
# AuthSrv_Authenticate() is used to clean up tokens that are no longer needed.
# End-Doc
sub AuthSrv_Unauthenticate {
    &LogAPIUsage();

    if ( defined( $ENV{KRB5CCNAME} ) ) {
        system("kdestroy >/dev/null 2>&1");
    }
}

1;

