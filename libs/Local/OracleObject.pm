#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#

=begin

Begin-Doc
Name: Local::OracleObject
Type: module
Description: object based interface to Oracle, child class derived from CommonDBObject

Comments: 

See the documentation for Local::CommonDBObject for the full set of routines. This documentation
only includes specific routines that are overridden by OracleObject and have different calling
conventions.

End-Doc

=cut

package Local::OracleObject;
use Exporter;
use DBI qw (:sql_types);
use Local::Env;
use Local::AuthSrv;
use Local::CurrentUser;
use Local::CommonDBObject;
use strict;

use vars qw (@ISA @EXPORT);
@ISA    = qw(Local::CommonDBObject Exporter);
@EXPORT = qw();

BEGIN {

    if ( $ENV{ORACLE_HOME} eq "" ) {
        $ENV{ORACLE_HOME} = "/usr/oracle";
    }

}

=begin
Begin-Doc
Name: checkerr
Access: private
Type: method
End-Doc
=cut

sub checkerr {
    my $self = shift;
    if ( $self->SQL_ErrorCode == 1013 ) {
        kill( 2, $$ );    # send this process the SIGINT that was
                          # trapped by the oracle library
    }
}

=begin
Begin-Doc
Name: SQL_OpenDatabase
Type: method
Description: opens a new database connection 
Syntax: $obj->SQL_OpenDatabase($db, %params)
Comments: Same syntax/behavior as routine in CommonDBObject.
End-Doc
=cut

sub SQL_OpenDatabase {
    my ( $self, $database, %info ) = @_;
    my ( $user, $pass, $host, $port );
    $user = $info{"user"};
    $pass = $info{"passwd"};

    if ( $database =~ m|^(.*)\*$|o ) {
        my $base   = $1;
        my $env    = &Local_Env();
        my %suffix = (
            "prod" => "p",
            "test" => "t",
            "dev"  => "d",
        );
        $database = $base . $suffix{$env};
    }

    if ( !defined $user or $user eq "" ) {
        $user = &Local_CurrentUser();
    }

    if ( !defined $pass or $pass eq "" ) {
        $pass = &AuthSrv_Fetch( user => $user, instance => "oracle" );
    }

    if (   !defined $database
        or !defined $self->SQL_CurrentDatabase
        or $self->SQL_CurrentDatabase ne $database )
    {
        if ( defined $self->dbhandle ) {
            $self->dbhandle->disconnect;
        }

        my @ora_default_signals = ("INT");
        $self->dbhandle(
            DBI->connect(
                "DBI:Oracle:$database", $user, $pass, { ora_connect_with_default_signals => \@ora_default_signals }
            )
        );

        if ( defined $self->dbhandle ) {
            $self->dbhandle->{PrintError}  = 0;
            $self->dbhandle->{ChopBlanks}  = 1;
            $self->dbhandle->{LongReadLen} = 32760;
        }
    }

    # Reset signal handler to default that was set in perl instead of the dorked up one provided by oracle
    # This looks like a no-op, but it gets the state in perl back to the real state
    $SIG{INT} = $SIG{INT};

    return defined( $self->dbhandle );
}

1;

