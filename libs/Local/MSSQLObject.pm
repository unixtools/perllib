#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: http://svn.unixtools.org/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#

=begin
Begin-Doc
Name: Local::MSSQLObject
Type: module
Description: object based interface to MSSQL, child class derived from CommonDBObject

Comments: 

See the documentation for Local::CommonDBObject for the full set of routines. This documentation
only includes specific routines that are overridden by this class and have different calling
conventions.

Note - for some databases, it may be necessary to set the "TDSVER" environment variable prior
to calling SQL_OpenDatabase routine.

End-Doc

=cut

package Local::MSSQLObject;
require 5.000;
use Exporter;
use DBI qw (:sql_types);
use Local::AuthSrv;
use Local::CurrentUser;
use Local::CommonDBObject;
use strict;

use vars qw (@ISA @EXPORT);
@ISA    = qw(Local::CommonDBObject Exporter);
@EXPORT = qw();

$ENV{SYBASE} = "/usr";

=begin
Begin-Doc
Name: SQL_OpenDatabase
Type: method
Description: opens a new database connection 
Syntax: $obj->SQL_OpenDatabase($db, %params)
Comments: See CommonDBObject for full details.
End-Doc
=cut

sub SQL_OpenDatabase {
    my ( $self, $database, %info ) = @_;
    my ( $user, $pass );
    $user = $info{"user"};
    $pass = $info{"passwd"};

    if ( !defined $user or $user eq "" ) {
        $user = &Local_CurrentUser();
    }

    if ( !defined $pass or $pass eq "" ) {
        $pass = &AuthSrv_Fetch( user => $user, instance => "mssql" );
    }

    if (   !defined $database
        or !defined $self->SQL_CurrentDatabase
        or $self->SQL_CurrentDatabase ne $database )
    {
        if ( defined $self->dbhandle ) {
            $self->dbhandle->disconnect;
        }

        $self->dbhandle( DBI->connect( "DBI:Sybase:server=$database", $user, $pass ) );
    }

    return defined( $self->dbhandle );
}

1;

