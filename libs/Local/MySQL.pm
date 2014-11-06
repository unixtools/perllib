#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#

=begin
Begin-Doc
Name: Local::MySQL
Type: module
Description: non object based access to MySQL
Comments: Don't use this module, it should be considered strongly discouraged. For documentation of it's routines
see the documentation for Local::MySQLObject. These routinese are just wrappers around the corresponding
method in Local::MySQLObject.

The usage of these routines is similar to those in Local::MySQLObject, just use them without the object
syntax:

&SQL_OpenDatabase(...) instead of $db->SQL_OpenDatabase(...);

Because there is no object associated with these, multiple database sessions can stomp on each other.

NEVER use this module in a library or perl module that may get re-used.

End-Doc
=cut

package Local::MySQL;
use Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

use Local::MySQLObject;
use Local::UsageLogger;

@ISA    = qw(Exporter);
@EXPORT = qw(
    SQL_Error
    SQL_HTMLError
    SQL_AssocArray
    SQL_OpenDatabase
    SQL_CloseDatabase
    SQL_OpenQuery
    SQL_CloseQuery
    SQL_ExecQuery
    SQL_DoQuery
    SQL_FetchRow
    SQL_ErrorCode
    SQL_ErrorString
    SQL_SerialNumber
    SQL_CurrentDatabase
    SQL_QuoteString
    SQL_Databases
    SQL_OpenBoundQuery
);

BEGIN {
    &LogAPIUsage();
}

# Global DB Handle
my $MYSQL_DBH = new Local::MySQLObject;

sub SQL_Error {
    return $MYSQL_DBH->SQL_Error(@_);
}

sub SQL_HTMLError {
    return $MYSQL_DBH->SQL_HTMLError(@_);
}

sub SQL_AssocArray {
    return $MYSQL_DBH->SQL_AssocArray(@_);
}

sub SQL_CurrentDatabase {
    return $MYSQL_DBH->SQL_CurrentDatabase(@_);
}

sub SQL_OpenDatabase {
    return $MYSQL_DBH->SQL_OpenDatabase(@_);
}

sub SQL_CloseDatabase {
    return $MYSQL_DBH->SQL_CloseDatabase(@_);
}

sub SQL_OpenBoundQuery {
    return $MYSQL_DBH->SQL_OpenBoundQuery(@_);
}

sub SQL_OpenQuery {
    return $MYSQL_DBH->SQL_OpenQuery(@_);
}

sub SQL_CloseQuery {
    return $MYSQL_DBH->SQL_CloseQuery(@_);
}

sub SQL_ExecQuery {
    return $MYSQL_DBH->SQL_ExecQuery(@_);
}

sub SQL_DoQuery {
    return $MYSQL_DBH->SQL_DoQuery(@_);
}

sub SQL_FetchRow {
    return $MYSQL_DBH->SQL_FetchRow(@_);
}

sub SQL_ErrorCode {
    return $MYSQL_DBH->SQL_ErrorCode(@_);
}

sub SQL_ErrorString {
    return $MYSQL_DBH->SQL_ErrorString(@_);
}

sub SQL_SerialNumber {
    return $MYSQL_DBH->SQL_SerialNumber(@_);
}

sub SQL_QuoteString {
    return $MYSQL_DBH->SQL_QuoteString(@_);
}

sub SQL_Databases {
    return $MYSQL_DBH->SQL_Databases(@_);
}

sub SQL_RowCount {
    return $MYSQL_DBH->SQL_RowCount(@_);
}

1;

