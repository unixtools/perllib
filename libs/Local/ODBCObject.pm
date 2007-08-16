
=begin
Begin-Doc
Name: Local::ODBCObject
Type: module
Description: Object based access to ODBC
End-Doc
=cut

package Local::ODBCObject;
require 5.000;
use Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use DBI;

@ISA    = qw(Exporter);
@EXPORT = qw();

use Local::AuthSrv;

sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my $tmp   = {};

    $tmp->{"dbhandle"}   = undef;
    $tmp->{"lastserial"} = undef;
    $tmp->{"debug"}      = undef;

    return bless $tmp, $class;
}

sub debug {
    my $self = shift;

    if (@_) {
        $self->{debug} = shift;
    }
    else {
        return $self->{debug};
    }
}

sub dbhandle {
    my $self = shift;

    if (@_) {
        $self->{dbhandle} = shift;
    }
    else {
        return $self->{dbhandle};
    }
}

sub SQL_Error {
    my ( $self, $qry ) = @_;

    print "------ ODBC Query Failed ------\n";
    print "$qry\n";
    print "-------------------------------------\n";

    print "\n";
    print "Error Code: ",   $self->SQL_ErrorCode,   "\n";
    print "Error Message:", $self->SQL_ErrorString, "\n";
    print "\n";
}

sub SQL_HTMLError {
    my ( $self, $qry ) = @_;

    print "<P><H1>ODBC Query Failed:</H1>\n";
    print "<PRE>$qry</PRE>\n";

    print "<UL>\n";
    print "<LI>Error Code: ",   $self->SQL_ErrorCode,   "\n";
    print "<LI>Error Message:", $self->SQL_ErrorString, "\n";
    print "</UL><P>\n";
}

sub SQL_CurrentDatabase {
    my $self = shift;

    if ( defined $self->dbhandle ) {

        #		return $self->dbhandle->{Name};
    }
    else {
        return undef;
    }
}

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
    }
    return defined( $self->dbhandle );
}

sub SQL_CloseDatabase {
    my $self = shift;
    if ( defined( $self->dbhandle ) ) {
        $self->dbhandle->disconnect;
        $self->dbhandle(undef);
    }
}

sub SQL_OpenBoundQuery {
    my ( $self, $qry ) = @_;
    my ($cid);

    $cid = $self->dbhandle->prepare($qry);
    return $cid;
}

sub SQL_OpenQuery {
    my ( $self, $qry, @params ) = @_;
    my ( $cid, $res );

    $cid = $self->dbhandle->prepare($qry);
    if ( defined($cid) ) {
        $res = $cid->execute(@params);
        if ($res) {
            return $cid;
        }
        else {
            return 0;
        }

        return $cid;
    }
    else {
        return 0;
    }
}

sub SQL_CloseQuery {
    my ( $self, $cid ) = @_;

    $cid->finish;
    undef $cid;
}

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
            $self->checkerr;
            return 0;
        }
    }

    return $res;
}

sub SQL_SerialNumber {
    my ($self) = @_;
    my ( $qry, $cid, $value );

    $qry = "select last_insert_id()";
    $cid = $self->SQL_OpenQuery($qry) || $self->SQL_HTMLError($qry);
    ($value) = $self->SQL_FetchRow($cid);
    $self->SQL_CloseQuery($cid);

    return $value;
}

sub SQL_FetchRow {
    my ( $self, $cid ) = @_;
    return $cid->fetchrow;
}

sub SQL_ErrorCode {
    return $DBI::err;
}

sub SQL_ErrorString {
    return $DBI::errstr;
}

sub SQL_QuoteString {
    my ( $self, $str ) = @_;
    return $self->dbhandle->quote($str);
}

sub SQL_Databases {
    return DBI->data_sources('mysql');
}

sub SQL_AssocArray {
    my ( $self, $WHERE, $TABLE, $KEY, @VALUE_FIELDS ) = @_;
    my ( %ASSOC_ARRAY, $KEYVAL, $VALVAL, $qry, $cid, $VALUES, @VAL_FIELDS );

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
    while ( my ( $KEYVAL, @VALVAL ) = $self->SQL_FetchRow($cid) ) {
        $ASSOC_ARRAY{$KEYVAL} = join( " ", @VALVAL );

    }
    $self->SQL_CloseQuery($cid);

    return %ASSOC_ARRAY;
}

sub SQL_FetchAllRows {
    my ( $self, $qry ) = @_;

    return $self->dbhandle->selectall_arrayref($qry);
}

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

sub SQL_Commit {
    my $self = shift;

    $self->dbhandle->commit;
    return 1;
}

sub SQL_RollBack {
    my $self = shift;

    $self->dbhandle->rollback;
    return 1;
}

sub SQL_AutoCommit {
    my $self = shift;
    my $val  = shift;

    if ( $val eq "" ) {
        $val = 1;
    }

    $self->dbhandle->{AutoCommit} = $val;

    return 1;
}

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

1;

