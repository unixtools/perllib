
=begin
Begin-Doc
Name: Local::MySQL
Type: module
Description: non object based access to MySQL
RCSId: $Header$
End-Doc
=cut

package Local::MySQL;
require 5.000;
use Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

use Local::MySQLObject;

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
	SQL_FetchRow
	SQL_ErrorCode
	SQL_ErrorString
	SQL_SerialNumber
	SQL_CurrentDatabase
	SQL_QuoteString
	SQL_Databases
	SQL_OpenBoundQuery
);

# Global DB Handle
my $MYSQL_DBH = new Local::MySQLObject;

sub SQL_Error
{
	return $MYSQL_DBH->SQL_Error(@_);
}

sub SQL_HTMLError
{
	return $MYSQL_DBH->SQL_HTMLError(@_);
}

sub SQL_AssocArray
{
	return $MYSQL_DBH->SQL_AssocArray(@_);
}

sub SQL_CurrentDatabase
{
	return $MYSQL_DBH->SQL_CurrentDatabase(@_);
}

sub SQL_OpenDatabase
{
	return $MYSQL_DBH->SQL_OpenDatabase(@_);
}

sub SQL_CloseDatabase
{
	return $MYSQL_DBH->SQL_CloseDatabase(@_);
}

sub SQL_OpenBoundQuery
{
	return $MYSQL_DBH->SQL_OpenBoundQuery(@_);
}

sub SQL_OpenQuery
{
	return $MYSQL_DBH->SQL_OpenQuery(@_);
}

sub SQL_CloseQuery
{
	return $MYSQL_DBH->SQL_CloseQuery(@_);
}

sub SQL_ExecQuery
{
	return $MYSQL_DBH->SQL_ExecQuery(@_);
}

sub SQL_FetchRow
{
	return $MYSQL_DBH->SQL_FetchRow(@_);
}

sub SQL_ErrorCode
{
	return $MYSQL_DBH->SQL_ErrorCode(@_);
}

sub SQL_ErrorString
{
	return $MYSQL_DBH->SQL_ErrorString(@_);
}

sub SQL_SerialNumber
{
	return $MYSQL_DBH->SQL_SerialNumber(@_);
}

sub SQL_QuoteString
{
	return $MYSQL_DBH->SQL_QuoteString(@_);
}

sub SQL_Databases
{
	return $MYSQL_DBH->SQL_Databases(@_);
}

sub SQL_RowCount
{
	return $MYSQL_DBH->SQL_RowCount(@_);
}

1;

