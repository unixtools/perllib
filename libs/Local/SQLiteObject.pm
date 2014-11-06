#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#

=begin

Begin-Doc
Name: Local::SQLiteObject
Type: module
Description: object based interface to SQLite db files, child class derived from CommonDBObject

Comments: 

See the documentation for Local::CommonDBObject for the full set of routines. This documentation
only includes specific routines that are overridden by this class and have different calling
conventions.

End-Doc

=cut

package Local::SQLiteObject;
use Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use DBI;
use Local::CommonDBObject;
use Local::UsageLogger;

@ISA    = qw(Local::CommonDBObject Exporter);
@EXPORT = qw();

BEGIN {
    &LogAPIUsage();
}

# Begin-Doc
# Name: SQL_OpenDatabase
# Description: opens connection to an SQLite file
# Syntax: $db->SQL_OpenDatabase("$mdbfilepath");
# End-Doc
sub SQL_OpenDatabase {
    my ( $self, $database, %info ) = @_;
    my ( $host, $port, $dsn );

    if ( $self->SQL_CurrentDatabase ne $database ) {
        if ( defined $self->dbhandle ) {
            $self->dbhandle->disconnect;
        }

        $dsn = "dbi:SQLite:dbname=$database";
        my $dbh = DBI->connect($dsn);
        return undef unless $dbh;

        $self->dbhandle($dbh);
        $self->dbhandle->{ChopBlanks} = 1;
        $self->dbhandle->{PrintError} = 0;
        $self->dbhandle->{RaiseError} = 0;    # don't generate a die
    }
    return defined( $self->dbhandle );
}

1;

