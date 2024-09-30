#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
#

=begin
Begin-Doc
Name: Local::MySQLObject
Type: module
Description: object-based interface to MySQL, child class derived from CommonDBObject

Comments: 

See the documentation for Local::CommonDBObject for the full set of routines. This documentation
only includes specific routines that are overridden by this class and have different calling
conventions.

End-Doc

=cut

package Local::MySQLObject;
use Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

use DBI;
use Local::AuthSrv;
use Local::CurrentUser;
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
Comments: Opens a connection to the database named in $db. Does not
re-open the database if the correct one is already open. Closes any currently
open database otherwise. Additional parameters can be passed using the
keys 'user','passwd', and 'host', whose values are the userid and password
respectively. Host defaults to 'localhost'. The 'user' and
'passwd' values default to the current user, and the password from that user's
'mysql' instance as retrieved by the AuthSrv API. If 'nopasswd' is non-zero
will connect without a password. If 'ssl' is passed with nonzero, ssl will be
required, otherwise optional. If 'ssl' is passed with zero, it will be disabled.

End-Doc
=cut

sub SQL_OpenDatabase {
    my ( $self, $database, %info ) = @_;
    my ( $user, $pass, $host, $port, $dsn );
    $user = $info{"user"};
    $pass = $info{"passwd"};
    $host = $info{"host"} || "localhost";
    $port = $info{"port"};

    if ( !defined($user) || $user eq "" ) {
        $user = &Local_CurrentUser();
    }

    if ( $info{nopasswd} ) {
        $pass = undef;
    }
    elsif ( !defined($pass) || $pass eq "" ) {
        $pass = &AuthSrv_Fetch( user => $user, instance => "mysql" );
    }

    if ( !defined( $self->SQL_CurrentDatabase ) || $self->SQL_CurrentDatabase ne $database ) {
        if ( defined $self->dbhandle ) {
            $self->dbhandle->disconnect;
        }

        $dsn = "DBI:mysql:database=$database";
        if ( defined($host) ) {
            $dsn .= ";host=$host";
        }

        if ( defined($port) ) {
            $dsn .= ";port=$port";
        }

        if ( -S "/var/lib/mysql/mysql.sock" ) {
            $dsn .= ";mysql_socket=/var/lib/mysql/mysql.sock";
        }
        elsif ( -S "/var/run/mysqld/mysqld.sock" ) {
            $dsn .= ";mysql_socket=/var/run/mysqld/mysqld.sock";
        }

        # Request that SSL be optionally enabled if supported
        if ( ! exists $info{ssl} ) {
            $dsn .= ";mysql_ssl_optional=1";
            $dsn .= ";mysql_ssl=1";
        }
        elsif ( $info{ssl} ) {
            $dsn .= ";mysql_ssl_optional=0";
            $dsn .= ";mysql_ssl=1";
        }
        elsif ( !$info{ssl} ) {
            $dsn .= ";mysql_ssl_optional=0";
            $dsn .= ";mysql_ssl=0";
        }

        my $dbh = DBI->connect( $dsn, $user, $pass );
        return undef unless $dbh;

        $self->dbhandle($dbh);
        $self->dbhandle->{PrintError} = 0;
        $self->dbhandle->{RaiseError} = 0;    # don't generate a die
    }
    return defined( $self->dbhandle );
}

=begin
Begin-Doc
Name: SQL_SerialNumber
Type: method
Description: returns the value assigned to last auto insert id number column
Syntax: $val = $obj->SQL_SerialNumber()
End-Doc
=cut

sub SQL_SerialNumber {
    my ($self) = @_;
    my ( $qry, $cid, $value );

    $qry = "select last_insert_id()";
    $cid = $self->SQL_OpenQuery($qry) || $self->SQL_HTMLError($qry);
    ($value) = $self->SQL_FetchRow($cid);
    $self->SQL_CloseQuery($cid);

    return $value;
}

1;

