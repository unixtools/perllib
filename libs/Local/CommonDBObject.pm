#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
#

=begin

Begin-Doc
Name: Local::CommonDBObject
Type: module
Description: an object-based interface to databases, a base class for other modules

Example: 

This is a base class for other database libraries to eliminate code duplication.

$db = new Local::CommonDBObject;

$db->SQL_OpenDatabase ("sal*") || $db->SQL_Error ("Can't open database");

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

package Local::CommonDBObject;
use Exporter;
use DBI qw (:sql_types);
use strict;
use Local::UsageLogger;

use vars qw (@ISA @EXPORT);
@ISA    = qw(Exporter);
@EXPORT = qw();

BEGIN {
    &LogAPIUsage();
}

=begin
Begin-Doc
Name: new
Type: method
Description: creates a new database object
Syntax: $obj = new Local::CommonDBObject
End-Doc
=cut

sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my $tmp   = {};

    $tmp->{"dbhandle"}   = undef;
    $tmp->{"lastserial"} = undef;
    $tmp->{"debug"}      = undef;

    &LogAPIUsage();

    bless $tmp, $class;

    return $tmp;
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
Name: checkerr
Access: private
Type: method
End-Doc
=cut

sub checkerr {
    my $self = shift;

    # This is a placeholder stub, some client libs will replace it.
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
Name: SQL_LastQuery
Type: method
Description: returns text of the last query executed
Syntax: $obj->SQL_LastQuery();
End-Doc
=cut

sub SQL_LastQuery {
    my $self = shift;
    return $self->{last_query};
}

=begin
Begin-Doc
Name: SQL_LastParams
Type: method
Description: returns ref to an array of last query parameters
Syntax: $obj->SQL_LastParams();
End-Doc
=cut

sub SQL_LastParams {
    my $self = shift;
    return $self->{last_params};
}

=begin
Begin-Doc
Name: SQL_Error
Type: method
Description: generates a textual error message with query error info
Syntax: $obj->SQL_Error($qry);
Comments: Prints out a plain text error message with '$qry' listed in
                the error message for reference.
End-Doc
=cut

sub SQL_Error {
    my ( $self, $qry ) = @_;

    print "--------- SQL Query Failed ----------\n";
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
Description: generates an html error message with query error info
Syntax: $obj->SQL_HTMLError($qry);
Comments: Prints out an HTML'ized error message with '$qry' listed in
                the error message for reference.
End-Doc
=cut

sub SQL_HTMLError {
    my ( $self, $qry ) = @_;

    print "<P><H1>SQL Query Failed:</H1>\n";
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
Description: commits the current open transaction
Syntax: $obj->SQL_Commit();
Comments: Commits a transaction when the script is operating in transaction
        mode (auto-commit is off).  See SQL_AutoCommit above for more
        information.
End-Doc
=cut

sub SQL_Commit {
    my $self = shift;

    return $self->dbhandle->commit;
}

=begin
Begin-Doc
Name: SQL_RollBack
Type: method
Description: rolls back the current transaction
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
Description: turns auto-commit feature on or off
Syntax: $obj->SQL_AutoCommit($val)
Comments: Turns auto-commit on or off (1 is on).  It is on by default, which
        causes each query to be committed as it is processed.  Turning
        auto-commit off causes the script to enter a transaction-based mode.
        While in transaction mode, changes to the database can only be caused by
        explicitly invoking SQL_Commit.  All activity after the previous commit
        can also be rolled back using SQL_RollBack.  Regular processing of
        commands can be reenabled by using this routing to turn on auto-commit.

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
Comments: non-implemented stub
End-Doc
=cut

sub SQL_OpenDatabase {
    die "This should not be called, should be overridden by the child class.";
}

=begin
Begin-Doc
Name: SQL_CloseDatabase
Type: method
Description: closes current database connection 
Syntax: $obj->SQL_CloseDatabase()
Comments: Closes the current database connection.  This is most useful when
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
    question marks as placeholders for values that will later be
    used with SQL_ExecQuery and the replacement values.

End-Doc
=cut

sub SQL_OpenBoundQuery {
    my ( $self, $qry ) = @_;
    my ($cid);

    $cid                          = $self->dbhandle->prepare($qry);
    $self->{last_query}           = $qry;
    $self->{last_params}          = [];
    $self->{cid_to_query}->{$cid} = $qry;
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

    $cid                 = $self->dbhandle->prepare($qry);
    $self->{last_query}  = $qry;
    $self->{last_params} = [@params];

    if ( defined($cid) ) {
        $self->{cid_to_query}->{$cid} = $qry;
        eval('my $foo = $cid->{NAME};');    # per Tim Bunce
        $res = $cid->execute(@params);

        if ($res) {
            return $cid;
        }
        else {
            $self->checkerr;
            undef( $self->{cid_to_query}->{$cid} );
            return 0;
        }
    }
    else {
        $self->checkerr;
        return 0;
    }
}

=begin
Begin-Doc
Name: SQL_OpenQueryExtra
Type: method
Description: opens a cursor to a new query
Syntax: $cid = $obj->SQL_OpenQueryExtra($qry, $properties, [@values])
Comments: This is the same as SQL_OpenQuery, but allows the passing hash of statement handle options such as ora_pers_lob
End-Doc
=cut

sub SQL_OpenQueryExtra {
    my ( $self, $qry, $props, @params ) = @_;
    my ( $cid, $res, $qcount );

    $cid = $self->dbhandle->prepare( $qry, $props );
    $self->{last_query}  = $qry;
    $self->{last_params} = [@params];

    if ( defined($cid) ) {
        $self->{cid_to_query}->{$cid} = $qry;
        eval('my $foo = $cid->{NAME};');    # per Tim Bunce
        $res = $cid->execute(@params);

        if ($res) {
            return $cid;
        }
        else {
            $self->checkerr;
            undef( $self->{cid_to_query}->{$cid} );
            return 0;
        }
    }
    else {
        $self->checkerr;
        return 0;
    }
}

=begin
Begin-Doc
Name: SQL_CloseQuery
Type: method
Description: closes an open query cursor
Syntax: $obj->SQL_CloseQuery($cid)
Comments: Closes query whose connection id is $cid.

End-Doc
=cut

sub SQL_CloseQuery {
    my ( $self, $cid ) = @_;

    $cid->finish;
    delete( $self->{cid_to_query}->{$cid} );

    # not sure on this
    #undef $self->{last_query};
    #undef $self->{last_params};
    undef $cid;
}

=begin
Begin-Doc
Name: SQL_ExecQuery
Type: method
Description: executes a SQL query
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
        $cid                 = $self->dbhandle->prepare($qry);
        $self->{last_query}  = $qry;
        $self->{last_params} = [@params];

        unless ( defined($cid) ) {
            $self->checkerr;
            return 0;
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
        return 0;
    }

    if ( !ref($qry) ) {
        $res = $cid->finish;
        unless ( $res >= 0 ) {
            $self->checkerr;
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
Description: executes a SQL query, returns a single row/record
Syntax: @res = $obj->SQL_DoQuery($qry, [@params])
Comments: Executes an SQL query.  This function can be used whenever an
        SQL command needs to be executed on the database server for single
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
End-Doc
=cut

sub SQL_AssocArray {
    my ( $self, $WHERE, $TABLE, $KEY, @VALUE_FIELDS ) = @_;
    my ( %ASSOC_ARRAY, $KEYVAL, $VALVAL, $qry, $cid, $VALUES, @VAL_FIELDS, @VALVAL );

    foreach $VALUES (@VALUE_FIELDS) {
        if ( $VALUES ne "" ) {
            push( @VAL_FIELDS, $VALUES );
        }
    }

    $VALUES = join( ", ", @VAL_FIELDS );

    $qry = "select distinct $KEY, $VALUES from $TABLE";
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
        the order specified in the SQL query.  The function returns 
        an empty array when no more rows remain.

End-Doc
=cut

sub SQL_FetchRow {
    my ( $self, $cid ) = @_;
    return $cid->fetchrow_array;
}

=begin
Begin-Doc
Name: SQL_FetchRowRef
Type: method
Description: fetches a single row, returns an array reference
Syntax: $rowarrayref = $obj->SQL_FetchRowRef($cid)
Comments: Returns a single row from a query as an array reference.  
        Columns are returned in the order specified in the SQL query.  
        The function returns undef when no more rows remain.

End-Doc
=cut

sub SQL_FetchRowRef {
    my ( $self, $cid ) = @_;
    return $cid->fetchrow_arrayref;
}

=begin
Begin-Doc
Name: SQL_FetchRow_Array
Type: method
Description: fetches a single row, returns an array reference
Syntax: $rowarrayref = $obj->SQL_FetchRow_Array($cid)
Comments: Returns a single row from a query as an array reference.  
        Columns are returned in the order specified in the SQL query.  
        The function returns undef when no more rows remain.

End-Doc
=cut

sub SQL_FetchRow_Array {
    my ( $self, $cid ) = @_;
    return $cid->fetchrow_arrayref;
}

=begin
Begin-Doc
Name: SQL_FetchRow_Hash
Type: method
Description: fetches a single row, returns hash reference, keys are column names
Syntax: $rowhashref = $obj->SQL_FetchRow_Hash($cid)
Comments: Returns a single row from a query as a hash reference.  
        The function returns undef when no more rows remain.

End-Doc
=cut

sub SQL_FetchRow_Hash {
    my ( $self, $cid ) = @_;

    my $row  = {};
    my @cols = @{ $cid->{NAME} };

    my $fetched = $cid->fetchrow_arrayref;
    if ($fetched) {
        @$row{@cols} = @$fetched;
        return $row;
    }
    else {
        return ();
    }
}

=begin
Begin-Doc
Name: SQL_FetchRow_LowerHash
Type: method
Description: fetches a single row, returns hash reference, keys are lc column names
Syntax: $rowhashref = $obj->SQL_FetchRow_LowerHash($cid)
Comments: Returns a single row from a query as a hash reference.  
        The function returns undef when no more rows remain.

End-Doc
=cut

sub SQL_FetchRow_LowerHash {
    my ( $self, $cid ) = @_;

    my $row = {};
    my @cols = map { lc($_) } @{ $cid->{NAME} };

    my $fetched = $cid->fetchrow_arrayref;
    if ($fetched) {
        @$row{@cols} = @$fetched;
        return $row;
    }
    else {
        return ();
    }
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
    my ( $self, $cid ) = @_;

    return $self->dbhandle->selectall_arrayref($cid);
}

=begin
Begin-Doc
Name: SQL_ErrorCode
Type: method
Description: returns an error code from last query
Syntax: $errcode = $obj->SQL_ErrorCode()
Comments: Returns an error code from the last query submitted.  Use
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
Description: returns an error message from last query
Syntax: $str = $obj->SQL_ErrorString()
Comments: Returns an error message from the last query submitted.  Use
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
Comments: Properly escapes special characters in a string so that it can
        be used as a column value in a SQL query.  This allows scripts to handle
        input that contains characters that are considered special by the
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
Description: returns a list of databases on the server
Syntax: @list = $obj->SQL_Databases()
Comments: Returns an array of database names that are available for
        connection.  The existence of a database in the return list does not
        necessarily permit access to the database.  You must have a valid userid
        and password to use a given database.
End-Doc
=cut

sub SQL_Databases {

    # stub only
    return ();
}

=begin
Begin-Doc
Name: SQL_ColumnInfo
Type: method
Description: returns information on columns returned by a query
Syntax: %info = $obj->SQL_ColumnInfo()
Comments: Returns a hash with keys numcols, colnames, coltypes, precision, and scale. All except numcols are
arrays. Column types, precision, and scale are going to be database driver specific. See DBI documentation
for more specifics. Typically use only the colnames element, or use all of the fields for comparison purposes.
End-Doc
=cut

sub SQL_ColumnInfo {
    my $self = shift;
    my $cid  = shift;
    my (%info);

    unless ( ref $cid ) {
        $cid = $self->sthandle;
    }

    $info{numcols}   = $cid->{NUM_OF_FIELDS};
    $info{colnames}  = [ @{ $cid->{NAME} } ];
    $info{coltypes}  = [ @{ $cid->{TYPE} } ];
    $info{precision} = [ @{ $cid->{PRECISION} } ];
    $info{scale}     = [ @{ $cid->{SCALE} } ];

    return %info;
}

=begin
Begin-Doc
Name: SQL_LowerColumns
Type: method
Description: returns a list of lowercased column names for the query
Syntax: @colnames = $obj->SQL_LowerColumns()
Comments: Returns an array with lowercased column names returned by the query
End-Doc
=cut

sub SQL_LowerColumns {
    my $self = shift;
    my $cid  = shift;

    unless ( ref $cid ) {
        $cid = $self->sthandle;
    }

    return map { lc($_) } @{ $cid->{NAME} };
}

=begin
Begin-Doc
Name: SQL_RowCount
Type: method
Description: returns the number of rows affected by the last SQL query
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

=begin
Begin-Doc
Name: SQL_SerialNumber
Type: method
Description: returns the value assigned to last auto insert id number column
Syntax: $val = $obj->SQL_SerialNumber()
End-Doc
=cut

sub SQL_SerialNumber {
    die "Routine not implemented for this database.";
}

1;

