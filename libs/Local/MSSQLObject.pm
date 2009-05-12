
=begin
Begin-Doc
Name: Local::MSSQLObject
Type: module
Description: Object based access to MSSQL/Sybase using freetds based code
Example: 

$db = new Local::MSSQLObject;

$db->SQL_OpenDatabase ("srv") || $db->SQL_Error ("Can't open database");

$qry = "insert into mytable (firstname, lastname, age) values ('John', 'Smith', 28)";
$db->SQL_ExecQuery ($qry) or $db->SQL_Error && die "$qry";


$qry = "delete from mytable where lastname = 'Smith' and firstname like 'Jo%' ";
$db->SQL_ExecQuery ($qry) or $db->SQL_Error && die "$qry";


$qry = "select firstname, lastname, age from mytable where lastname like 'Joh%' order by lastname, firstname";
$cid = $db->SQL_OpenQuery ($qry) or $db->SQL_Error && die "$qry";
while (($first, $last, $age) = $db->SQL_FetchRow (<b>$cid</b>))
{
        print "$first $last is $age years old\n";
}
$db->SQL_CloseQuery (<b>$cid</b>);



$qry = "insert into mytable (firstname, lastname, age) values (?, ?, ?)";
$cid = $db->SQL_OpenBoundQuery ($qry);
$db->SQL_ExecQuery ($cid, "John", "Jones", 99) or $db->SQL_Error;
$db->SQL_ExecQuery ($cid, "Jerry", "Smith", 47) or $db->SQL_Error;
$db->SQL_CloseQuery ($cid);


$db->SQL_CloseDatabase();
End-Doc

=cut

package Local::MSSQLObject;
require 5.000;
use Exporter;
use DBI qw (:sql_types);
use Local::AuthSrv;
use strict;

use vars qw (@ISA @EXPORT);
@ISA    = qw(Exporter);
@EXPORT = qw();

BEGIN {
}

$ENV{SYBASE} = "/usr";

=begin
Begin-Doc
Name: new
Type: method
Description: creates new oracle object
Syntax: $obj = new Local::MSSQLObject
End-Doc
=cut

sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my $tmp   = {};

    $tmp->{"dbhandle"}   = undef;
    $tmp->{"lastserial"} = undef;
    $tmp->{"debug"}      = undef;

    return bless $tmp, $class;
}

=begin
Begin-Doc
Name: debug
Access: private
Type: method
End-Doc
=cut

sub debug {
    my $self = shift;

    if (@_) {
        $self->{debug} = shift;
    }
    else {
        return $self->{debug};
    }
}

=begin
Begin-Doc
Name: dbhandle
Access: private
Type: method
End-Doc
=cut

sub dbhandle {
    my $self = shift;

    if (@_) {
        $self->{dbhandle} = shift;
    }
    else {
        return $self->{dbhandle};
    }
}

=begin
Begin-Doc
Name: sthandle
Access: private
Type: method
End-Doc
=cut

sub sthandle {
    my $self = shift;
    if (@_) {
        $self->{sthandle} = shift;
    }
    else {
        return $self->{sthandle};
    }
}

=begin
Begin-Doc
Name: SQL_Error
Type: method
Description: generates a textual error message with oracle error info
Syntax: $obj->SQL_Error($qry);
Comments: Prints out an plain text error message with '$qry' listed in
                the error message for reference.
End-Doc
=cut

sub SQL_Error {
    my ( $self, $qry ) = @_;

    print "------ MSSQL SQL Query Failed ------\n";
    print "$qry\n";
    print "-------------------------------------\n";

    print "\n";
    print "Error Code: ",   $self->SQL_ErrorCode,   "\n";
    print "Error Message:", $self->SQL_ErrorString, "\n";
    print "\n";
}

=begin
Begin-Doc
Name: SQL_HTMLError
Type: method
Description: generates a html error message with oracle error info
Syntax: $obj->SQL_HTMLError($qry);
Comments: Prints out an HTML'ized error message with '$qry' listed in
                the error message for reference.
End-Doc
=cut

sub SQL_HTMLError {
    my ( $self, $qry ) = @_;

    print "<P><H1>MSSQL SQL Query Failed:</H1>\n";
    print "<PRE>$qry</PRE>\n";

    print "<UL>\n";
    print "<LI>Error Code: ",   $self->SQL_ErrorCode,   "\n";
    print "<LI>Error Message:", $self->SQL_ErrorString, "\n";
    print "</UL><P>\n";
}

=begin
Begin-Doc
Name: SQL_Commit
Type: method
Description: commits current open transaction
Syntax: $obj->SQL_Commit();
Comments: Commits a transaction when the script is operating in transaction
        mode (auto-commit is off).  See SQL_AutoCommit above for more
        information.
End-Doc
=cut

sub SQL_Commit {
    my $self = shift;

    $self->dbhandle->commit;
    return 1;
}

=begin
Begin-Doc
Name: SQL_RollBack
Type: method
Description: rolls back current transaction
Syntax: $obj->SQL_RollBack()
Comments: Rolls back a transaction when the script is operating in transaction
        mode (auto-commit is off).  See SQL_AutoCommit above for more
        information.

End-Doc
=cut

sub SQL_RollBack {
    my $self = shift;

    $self->dbhandle->rollback;
    return 1;
}

=begin
Begin-Doc
Name: SQL_AutoCommit
Type: method
Description: turns autocommit on or off
Syntax: $obj->SQL_AutoCommit($val)
Comments: Turns auto-commit on or off (1 is on).  It is on by default, which
        causes each query to be committed as it is processed.  Turning
        auto-commit off causes the script to enter a transaction-based mode.
        While in transaction mode, changes to the database can only be caused by
        explicity invoking SQL_Commit.  All activity after the previous commit
        can also be rolled back using SQL_RollBack.  Regular processing of
        commands can be reenabled by using this routing to turn on auto-commit.
        It is recommended that only experienced database programmers use this
        function.  Please contact the Database Administrator if you feel at all
        uncertain about transaction-based programming.

End-Doc
=cut

sub SQL_AutoCommit {
    my $self = shift;
    my $val  = shift;

    if ( $val eq "" ) {
        $val = 1;
    }

    $self->dbhandle->{AutoCommit} = $val;

    return 1;
}

=begin
Begin-Doc
Name: SQL_CurrentDatabase
Type: method
Description: returns name of current database
Syntax: $dbname = $obj->SQL_CurrentDatabase()
Comments: 
End-Doc
=cut

sub SQL_CurrentDatabase {
    my $self = shift;

    if ( defined $self->dbhandle ) {
        return $self->dbhandle->{Name};
    }
    else {
        return undef;
    }
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
'oracle' instance as retrieved by the AuthSrv API.

End-Doc
=cut

sub SQL_OpenDatabase {
    my ( $self, $database, %info ) = @_;
    my ( $user, $pass );
    $user = $info{"user"};
    $pass = $info{"passwd"};

    if ( !defined $user or $user eq "" ) {

        # Get current uid and retrieve user name
        $user = ( getpwuid($>) )[0];
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

        $self->dbhandle(
            DBI->connect( "DBI:Sybase:server=$database", $user, $pass ) );
    }

    return defined( $self->dbhandle );
}

=begin
Begin-Doc
Name: SQL_CloseDatabase
Type: method
Description: closes current database connection 
Syntax: $obj->SQL_CloseDatabase()
Comments: Closes the current datbase connection.  This is most useful when
        connecting to more than one database server is necessary and the
        object-oriented routines are not being used.

End-Doc
=cut

sub SQL_CloseDatabase {
    my $self = shift;
    if ( defined( $self->dbhandle ) ) {
        $self->dbhandle->disconnect;
        $self->dbhandle(undef);
    }
}

=begin
Begin-Doc
Name: SQL_OpenBoundQuery
Type: method
Description: opens a cursor but delays processing of parameters
Syntax: $cid = $obj->SQL_OpenBoundQuery($qry);
Comments: Opens a query using bound parameters.  Bound parameters use
    question marks as place holders for values that that will later be
    used with SQL_ExecQuery and the replacement values.

End-Doc
=cut

sub SQL_OpenBoundQuery {
    my ( $self, $qry ) = @_;
    my ($cid);

    $cid = $self->dbhandle->prepare($qry);
    return $cid;
}

=begin
Begin-Doc
Name: SQL_OpenQuery
Type: method
Description: opens a cursor to a new query
Syntax: $cid = $obj->SQL_OpenQuery($qry, [@values])
Comments: Submits a query string, returning a connection id which can later be
        used in other calls to access the rows returned by the query.
        SQL_OpenQuery is only needed for retrieving one or more rows from a
        query.  SQL_ExecQuery (see below) should be used for all other data
        manipulation.  If the query can not be opened, SQL_OpenQuery returns
        0 to indicate failure. @values can optionally be specified if any
        bound parameters are used in the query.
End-Doc
=cut

sub SQL_OpenQuery {
    my ( $self, $qry, @params ) = @_;
    my ( $cid, $res, $qcount );

    $cid = $self->dbhandle->prepare($qry);

    if ( defined($cid) ) {
        eval('my $foo = $cid->{NAME};');    # per Tim Bunce
        $res = $cid->execute(@params);

        if ($res) {
            return $cid;
        }
        else {
            return 0;
        }
    }
    else {
        return 0;
    }
}

=begin
Begin-Doc
Name: SQL_CloseQuery
Type: method
Description: closes open query cursor
Syntax: $obj->SQL_CloseQuery($cid)
Comments: Closes query whose connection id is $cid.

End-Doc
=cut

sub SQL_CloseQuery {
    my ( $self, $cid ) = @_;

    $cid->finish;
    undef $cid;
}

=begin
Begin-Doc
Name: SQL_ExecQuery
Type: method
Description: executes a sql query
Syntax: $res = $obj->SQL_ExecQuery($qry, [@params])
Comments: Executes an SQL query.  This function should be used whenever an
        SQL command needs to be executed on the database server for any purpose
        other than retrieving data.  If the optional array of values is
        included, all instances of &quot;?&quot; in the query will be
        replaced with the corresponding value from @values.  The number of
        array elements must match the number of question marks.  The array of
        replacement values should be used if a bound query was created using
        SQL_OpenBoundQuery (see above).
        SQL_ExecQuery returns 1 if successful and 0 otherwise.

End-Doc
=cut

sub SQL_ExecQuery {
    my ( $self, $qry, @params ) = @_;
    my ( $res, $cid );

    if ( !ref($qry) ) {
        $cid = $self->dbhandle->prepare($qry);
        unless ( defined($cid) ) {
            return 0;
        }
    }
    else {
        $cid = $qry;
    }

    $res = $cid->execute(@params);
    unless ($res) {
        return 0;
    }

    if ( !ref($qry) ) {
        $res = $cid->finish;
        unless ( $res >= 0 ) {
            return 0;
        }
    }

    $self->sthandle($cid);
    return $res;
}

=begin
Begin-Doc
Name: SQL_DoQuery
Type: method
Description: executes a sql query, returns single row
Syntax: @res = $obj->SQL_DoQuery($qry, [@params])
Comments: Executes an SQL query.  This function can be used whenever an
        SQL command needs to be executed on the database server for a single
        record retrieval.  If the optional array of values is
        included, all instances of &quot;?&quot; in the query will be
        replaced with the corresponding value from @values.  The number of
        array elements must match the number of question marks.  The array of
        replacement values should be used if a bound query was created using
        SQL_OpenBoundQuery (see above).
        SQL_DoQuery returns undef if failure, and row contents otherwise.

End-Doc
=cut

sub SQL_DoQuery {
    my ( $self, $qry, @params ) = @_;
    my ( $res, $cid );
    my @results;

    if ( !ref($qry) ) {
        $cid                 = $self->dbhandle->prepare($qry);
        $self->{last_query}  = $qry;
        $self->{last_params} = [@params];

        unless ( defined($cid) ) {
            $self->checkerr;
            return;
        }
    }
    else {
        $cid                 = $qry;
        $self->{last_query}  = $self->{cid_to_query}->{$cid};
        $self->{last_params} = [@params];
    }

    $res = $cid->execute(@params);
    unless ($res) {
        $self->checkerr;
        return;
    }

    @results = $cid->fetchrow;

    if ( !ref($qry) ) {
        $res = $cid->finish;
        unless ( $res >= 0 ) {
            $self->checkerr;
            return;
        }
    }

    # Record the sthandle of the last executed query
    $self->sthandle($cid);

    return @results;
}

=begin
Begin-Doc
Name: SQL_AssocArray
Type: method
Description: runs query and returns keyed hash, useful for lookup tables
Syntax: %hash = $obj->SQL_AssocArray($where, $table, $key, $valuefields);
Comments: Don't use this.
End-Doc
=cut

sub SQL_AssocArray {
    my ( $self, $WHERE, $TABLE, $KEY, @VALUE_FIELDS ) = @_;
    my ( %ASSOC_ARRAY, $KEYVAL, $VALVAL, $qry, $cid, $VALUES, @VAL_FIELDS,
        @VALVAL );

    foreach $VALUES (@VALUE_FIELDS) {
        if ( $VALUES ne "" ) {
            push( @VAL_FIELDS, $VALUES );
        }
    }

    $VALUES = join( ", ", @VAL_FIELDS );

    $qry = "select unique $KEY, $VALUES from $TABLE";
    if ( $WHERE ne "" ) {
        $qry .= " where " . $WHERE;
    }

    $cid = $self->SQL_OpenQuery("$qry") || $self->SQL_HTMLError($qry);
    while ( ( $KEYVAL, @VALVAL ) = $self->SQL_FetchRow($cid) ) {
        $ASSOC_ARRAY{$KEYVAL} = join( " ", @VALVAL );

    }
    $self->SQL_CloseQuery($cid);

    return %ASSOC_ARRAY;
}

=begin
Begin-Doc
Name: SQL_FetchRow
Type: method
Description: fetches a single row
Syntax: @row = $obj->SQL_FetchRow($cid)
Comments: Returns a single row from a query.  Columns are returned in
        the order specified in the SQL query.  The function returns a value of
        &quot;undef&quot; when no more rows remain.

End-Doc
=cut

sub SQL_FetchRow {
    my ( $self, $cid ) = @_;
    return $cid->fetchrow;
}

=begin
Begin-Doc
Name: SQL_FetchAllRows
Type: method
Description: returns array ref with all rows from a query
Syntax: $arrayref = $obj->SQL_FetchAllRows($cid);
Comments: 
End-Doc
=cut

sub SQL_FetchAllRows {
    my ( $self, $qry ) = @_;

    return $self->dbhandle->selectall_arrayref($qry);
}

=begin
Begin-Doc
Name: SQL_ErrorCode
Type: method
Description: returns error code from last query
Syntax: $errcode = $obj->SQL_ErrorCode()
Comments: Returns an error code from last query submitted.  Use
        SQL_ErrorString to see the text of the corresponding error message.

End-Doc
=cut

sub SQL_ErrorCode {
    return $DBI::err;
}

=begin
Begin-Doc
Name: SQL_ErrorString
Type: method
Description: returns error message from last query
Syntax: $str = $obj->SQL_ErrorString()
Comments: Returns an error message from last query submitted.  Use
        SQL_ErrorCode to see the corresponding error code.

End-Doc
=cut

sub SQL_ErrorString {
    return $DBI::errstr;
}

=begin
Begin-Doc
Name: SQL_QuoteString
Type: method
Description: quotes a string for safe use in a query
Syntax: $safe = $obj->SQL_QuoteString($str)
Comments: Properly escapes special characters in an string so that it can
        be used as a column value in an SQL query.  This allows scripts to handle
        input which contains characters that are considered special by the
        database engine.  The resulting safe string is enclosed in single
        quotation marks.

End-Doc
=cut

sub SQL_QuoteString {
    my ( $self, $str ) = @_;
    return $self->dbhandle->quote($str);
}

=begin
Begin-Doc
Name: SQL_Databases
Type: method
Description: returns list of databases on the server
Syntax: @list = $obj->SQL_Databases()
Comments: Returns an array of database names that are available for
        connection.  The existence of a database in the return list does not
        necessarily permit access to the database.  You must have a valid userid
        and password to use a given database.
End-Doc
=cut

sub SQL_Databases {
    return DBI->data_sources('Sybase');
}

=begin
Begin-Doc
Name: SQL_FetchAllRows
Type: method
Description: returns array ref with all rows from a query
Syntax: $arrayref = $obj->SQL_FetchAllRows($cid);
Comments: 
End-Doc
=cut

sub SQL_ColumnInfo {
    my $self = shift;
    my $cid  = shift;
    my (%info);

    $info{numcols}   = $cid->{NUM_OF_FIELDS};
    $info{colnames}  = [ @{ $cid->{NAME} } ];
    $info{coltypes}  = [ @{ $cid->{TYPE} } ];
    $info{precision} = [ @{ $cid->{PRECISION} } ];
    $info{scale}     = [ @{ $cid->{SCALE} } ];

    return %info;
}

=begin
Begin-Doc
Name: SQL_RowCount
Type: method
Description: returns number of rows affected by last SQL query
Syntax: $obj->SQL_RowCount()
Comments: Returns the number of rows affected by the last SQL query.  This
        function is intended for use with SQL_ExecQuery.  Results may be
        unreliable if used with SQL_OpenQuery and SQL_FetchRow.  If you wish to
        determine the number of rows returned by a given query, it is
        recommended that they be counted with a loop counter as they are
        retrieved from the database.

End-Doc
=cut

sub SQL_RowCount {
    my $self = shift;
    my $cid  = shift;

    unless ( ref $cid ) {
        $cid = $self->sthandle;
    }

    if ($cid) {
        return int( $cid->rows );
    }
    else {
        return 0;
    }
}

1;

