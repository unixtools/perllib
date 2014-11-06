#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#

=begin
Begin-Doc
Name: Local::ODBCObject
Type: module
Description: object based interface to ODBC database, child class derived from CommonDBObject

Comments: 

See the documentation for Local::CommonDBObject for the full set of routines. This documentation
only includes specific routines that are overridden by this class and have different calling
conventions.

This module is currently windows-specific.

End-Doc
=cut

package Local::ODBCObject;
use Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use DBI;
use Local::AuthSrv;
use Local::CommonDBObject;
use Local::UsageLogger;

@ISA    = qw(Local::CommonDBObject Exporter);
@EXPORT = qw();

BEGIN {
    &LogAPIUsage();
}

=begin
Begin-Doc
Name: SQL_OpenDatabase
Type: method
Description: opens a new database connection 
Syntax: $obj->SQL_OpenDatabase($db, %params)
Comments: Opens a connection to the database named in $dbname. Does not
re-open the database if the correct one is already open. Closes any currently
open database otherwise. Additional parameters can be passed using the
keys 'user' and 'passwd', whose values are the userid and password
respectively. The 'user' and
'passwd' values default to the current user, and the password from that user's
'odbc' instance as retrieved by the AuthSrv API.

End-Doc
=cut

sub SQL_OpenDatabase {
    my ( $self, $database, %info ) = @_;
    my ( $user, $pass, $host, $port, $dsn );
    $user = $info{"user"};
    $pass = $info{"passwd"};

    if ( $user eq "" ) {

        # Get current uid and retrieve user name
        $user = ( getpwuid($>) )[0];
    }

    if ( $info{nopasswd} ) {
        $pass = undef;
    }
    elsif ( $pass eq "" ) {
        $pass = &AuthSrv_Fetch( user => $user, instance => "odbc" );
    }

    if ( $self->SQL_CurrentDatabase ne $database ) {
        if ( defined $self->dbhandle ) {
            $self->dbhandle->disconnect;
        }

        $dsn = "DBI:ODBC:DSN=$database";

        my $dbh = DBI->connect( $dsn, $user, $pass );
        return undef unless $dbh;

        $self->dbhandle($dbh);
        $self->dbhandle->{ChopBlanks} = 1;
        $self->dbhandle->{PrintError} = 0;
        $self->dbhandle->{RaiseError} = 0;    # don't generate a die

        $self->dbhandle->{LongReadLen} = 32760;
        $self->dbhandle->{LongTruncOk} = 1;
    }
    return defined( $self->dbhandle );
}

1;

