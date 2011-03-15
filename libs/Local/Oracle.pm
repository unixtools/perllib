#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: http://svn.unixtools.org/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#


=begin

Begin-Doc
Name: Local::Oracle
Type: module
Description: non-object based UMR local interface to Oracle
Comments: Don't use this module, it should be considered strongly discouraged. For documentation of it's routines
see the documentation for Local::OracleObject. These routinese are just wrappers around the corresponding
method in Local::OracleObject.

The usage of these routines is similar to those in Local::OracleObject, just use them without the object
syntax:

&SQL_OpenDatabase(...) instead of $db->SQL_OpenDatabase(...);

Because there is no object associated with these, multiple database sessions can stomp on each other.

NEVER use this module in a library or perl module that may get re-used.

End-Doc

=cut

package Local::Oracle;
require 5.000;
use Exporter;
use DBI;
use Local::OracleObject;

@ISA    = qw(Exporter);
@EXPORT = qw(SQL_Error SQL_HTMLError SQL_AssocArray
  SQL_OpenDatabase SQL_CloseDatabase SQL_OpenQuery
  SQL_CloseQuery SQL_ExecQuery SQL_DoQuery SQL_FetchRow SQL_ErrorCode
  SQL_ErrorString SQL_SerialNumber SQL_CurrentDatabase
  SQL_QuoteString SQL_Databases SQL_OpenBoundQuery
  SQL_Commit SQL_RollBack SQL_AutoCommit SQL_FetchAllRows
  SQL_RowCount
  SQL_ColumnInfo
);

$DBH = new Local::OracleObject;

sub SQL_Error {
    return $DBH->SQL_Error(@_);
}

sub SQL_HTMLError {
    return $DBH->SQL_HTMLError(@_);
}

sub SQL_AssocArray {
    return $DBH->SQL_AssocArray(@_);
}

sub SQL_Commit {
    return $DBH->SQL_Commit(@_);
}

sub SQL_RollBack {
    return $DBH->SQL_RollBack(@_);
}

sub SQL_AutoCommit {
    return $DBH->SQL_AutoCommit(@_);
}

sub SQL_CurrentDatabase {
    return $DBH->SQL_CurrentDatabase(@_);
}

sub SQL_OpenDatabase {
    return $DBH->SQL_OpenDatabase(@_);
}

sub SQL_CloseDatabase {
    return $DBH->SQL_CloseDatabase(@_);
}

sub SQL_OpenBoundQuery {
    return $DBH->SQL_OpenBoundQuery(@_);
}

sub SQL_OpenQuery {
    return $DBH->SQL_OpenQuery(@_);
}

sub SQL_CloseQuery {
    return $DBH->SQL_CloseQuery(@_);
}

sub SQL_ExecQuery {
    return $DBH->SQL_ExecQuery(@_);
}

sub SQL_DoQuery {
    return $DBH->SQL_DoQuery(@_);
}

sub SQL_FetchRow {
    return $DBH->SQL_FetchRow(@_);
}

sub SQL_FetchAllRows {
    return $DBH->SQL_FetchAllRows(@_);
}

sub SQL_ErrorCode {
    return $DBH->SQL_ErrorCode(@_);
}

sub SQL_ErrorString {
    return $DBH->SQL_ErrorString(@_);
}

sub SQL_SerialNumber {
    return $DBH->SQL_SerialNumber(@_);
}

sub SQL_QuoteString {
    return $DBH->SQL_QuoteString(@_);
}

sub SQL_Databases {
    return $DBH->SQL_Databases(@_);
}

sub SQL_RowCount {
    return $DBH->SQL_RowCount(@_);
}

sub SQL_ColumnInfo {
    return $DBH->SQL_ColumnInfo(@_);
}
1;
