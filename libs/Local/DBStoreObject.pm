#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
#

=begin
Begin-Doc
Name: Local::DBStoreObject
Type: module
Description: Module to allow for storing large quantities of data in non-lob tables
Comments:


This module provides a set of tools for storing and retrieving arbitrarily large text items
without the need for data types specific to a particular database engine. The intent is to
allow application developers to simulate comment or memo fields while providing a simple
migration path to other database engines.

Prior to using this module, you will need to have a table created to hold the text blocks. The
following structure should be used to create the table.

create table $tablename
(
	id integer not null,
	seq integer not null,
	data char ($chunksize)
);

The id column is used to identify a particular record to store, update, or fetch. If your
unique identiers do not lend themselves to integer values, you may safely change the data type
of id during the table creation process. This will not affect the performance of the DBStore
object.

The seq column must be an integer. It is used to reconstruct the text data when it is fetched
from the database.

The data column is used to hold the actual text data. The size of this column is determined by
the chunksize variable. Chunksize is an integer value and represents the number of characters
that will be stored in an average block of text. For example, if you want to store text
objects in 800 character blocks, chunksize will be 800.


Example:

use Local::DBStoreObject;

$dbstore = new Local::DBStoreObject(
    table => $tablename,
    chunksize => $chunksize,
    db => $dbobject);
$dbstore->store ($id, $text);
$text = $dbstore->fetch ($id);
$dbstore->update ($id, $text);
$dbstore->docache ($true_false);
$true_false = $dbstore->docache;

End-Doc
=cut

package Local::DBStoreObject;
use Exporter;
use DBI;
use vars qw(@ISA @EXPORT);
use Carp;
use strict;
use MIME::Base64;

@ISA    = qw(Exporter);
@EXPORT = qw();

my $MIME_MAGIC_HEADER = "\%MIME_ENCODE\%";

=begin
Begin-Doc
Name: new
Type: method
Description: Creates new DBStoreObject
Syntax: $dbstore = new Local::DBStoreObject (%parms)
Comments: %parms has manditory keys "table", "db", and optional keys
Comments: "id_field", "seq_field", "data_field", "cache", "chunksize", "debug", "disable_mime"
Comments: fields default to 'id', 'seq', and 'data'
Comments: chunksize defaults to 4000
Comments: cache defaults to 1/enabled
Comments: disable_mime defaults to 0 and should generally not be used
End-Doc
=cut

sub new {
    my $self = shift;
    my $class = ref($self) || $self;

    my %opts = @_;

    my $tmp = {};

    $tmp->{table}      = $opts{table}      || die "must specify table";
    $tmp->{chunksize}  = $opts{chunksize}  || 4000;
    $tmp->{id_field}   = $opts{id_field}   || "id";
    $tmp->{seq_field}  = $opts{seq_field}  || "seq";
    $tmp->{data_field} = $opts{data_field} || "data";
    $tmp->{db}         = $opts{db}         || die "must specify db object";
    $tmp->{disable_mime} = $opts{disable_mime};
    $tmp->{cache}        = {};
    if ( exists( $opts{cache} ) && !$opts{cache} ) {
        $tmp->{cache} = undef;
    }
    if ( exists( $opts{debug} ) ) {
        $tmp->{debug} = $opts{debug};
    }
    else {
        $tmp->{debug} = 0;
    }

    $tmp->{ins_handle} = undef;
    $tmp->{_cache}     = {};

    return bless $tmp, $class;
}

=begin
Begin-Doc
Name: debug
Type: method
Description: set or retrieve debugging flag
End-Doc
=cut

sub debug {
    my $self = shift;

    if ( scalar @_ ) {
        $self->{debug} = shift;
    }
    else {
        return $self->{debug};
    }
}

=begin
Begin-Doc
Name: fetch
Type: method
Description: retrieve entry from table
Syntax: $text = $dbstore->fetch ($id);

Comments: This function retrieves the text associated with the id variable from the database
and stores it in the text variable.

End-Doc
=cut

sub fetch {
    my $self       = shift;
    my $id         = shift;
    my $table      = $self->{table};
    my $cache      = $self->{cache};
    my $id_field   = $self->{id_field};
    my $seq_field  = $self->{seq_field};
    my $data_field = $self->{data_field};
    my $db         = $self->{db};

    if ( $cache && defined( $cache->{$id} ) ) {
        return $cache->{$id};
    }
    else {
        my ( $qry, $cid, $data, $seq, $chunk );

        # Retrieval query
        $qry = "select $seq_field,$data_field from $table where $id_field=? order by $seq_field";
        $cid = $db->SQL_OpenBoundQuery($qry);
        $db->SQL_ExecQuery( $cid, $id );

        # Join the fields together
        $data = "";
        while ( ( $seq, $chunk ) = $db->SQL_FetchRow($cid) ) {
            $self->{debug} && print "read chunk($seq) for id($id) of length(", length($chunk), ")\n";
            $data .= $chunk;
        }
        $db->SQL_CloseQuery($cid);
        $self->{debug} && print "read total for id($id) of length(", length($data), ")\n";

        #
        # Check if we prefixed the stored data with a format marker indicating
        # mime encoding, and decode if we did.
        #
        if ( index( $data, $MIME_MAGIC_HEADER ) == 0 ) {
            eval { $data = decode_base64( substr( $data, length($MIME_MAGIC_HEADER) ) ); };
            $self->{debug} && print "read decoded for id($id) of length(", length($data), ")\n";
        }

        # Cache and return result
        if ($cache) {
            $cache->{$id} = $data;
        }
        return $data;
    }
}

=begin
Begin-Doc
Name: delete
Type: method
Description: deletes entry from table
Syntax: $dbstore->delete($id);
End-Doc
=cut

sub delete {
    my $self = shift;
    my $id   = shift;

    my $table    = $self->{"table"};
    my $cache    = $self->{"cache"};
    my $id_field = $self->{id_field};
    my $db       = $self->{db};
    my $qry;

    # Delete from cache if caching enabled
    if ($cache) {
        delete( $cache->{$id} );
    }

    # Delete from database
    $qry = "delete from $table where $id_field=?";
    $db->SQL_ExecQuery( $qry, $id ) || $db->SQL_Error($qry);
}

=begin
Begin-Doc
Name: update
Type: method
Description: updates entry in table
Syntax: $dbstore->update ($id, $text);

Comments: This does the same thing as the store method.

End-Doc
=cut

sub update {
    my $self   = shift;
    my $id     = shift;
    my $string = shift;

    $self->store( $id, $string );
}

=begin
Begin-Doc
Name: store
Type: method
Description: stores entry in table
Syntax: $dbstore->store ($id, $text);

Comments: This function replaces data associated with the id variable with the data supplied
in the text variable. Any existing data associated with the given id is deleted, guaranteeing
the uniqueness of the id variable.

End-Doc
=cut

sub store {
    my $self   = shift;
    my $id     = shift;
    my $string = shift;

    my $table      = $self->{"table"};
    my $chunksize  = $self->{"chunksize"};
    my $cache      = $self->{"cache"};
    my $debug      = $self->{"debug"};
    my $id_field   = $self->{id_field};
    my $seq_field  = $self->{seq_field};
    my $data_field = $self->{data_field};
    my $db         = $self->{db};

    my ( $i, $chunk, $qry );
    my $cid = $self->{ins_handle};

    # Make sure any previous one is gone
    $self->delete($id);

    if ($cache) {
        $cache->{$id} = $string;
    }

    #
    # Unless we have disabled mime encoding, mime encode the string before inserting into db
    #
    $self->{debug} && print "store total for id($id) of length(", length($string), ")\n";
    unless ( $self->{disable_mime} ) {
        $string = $MIME_MAGIC_HEADER . encode_base64($string);
        $self->{debug} && print "store decoded for id($id) of length(", length($string), ")\n";
    }

    #
    # Split up into chunks and store each chunk in database
    #
    my $seq    = 0;
    my $offset = 0;
    while ( $offset < length($string) ) {
        my $chunk = substr( $string, $offset, $chunksize );
        $offset += $chunksize;

        unless ($cid) {
            if ( $self->debug ) {
                print "Opening Bound Query\n";
            }
            $qry                = "insert into $table($id_field,$seq_field,$data_field) values (?,?,?)";
            $cid                = $db->SQL_OpenBoundQuery($qry) || $db->SQL_Error($qry);
            $self->{ins_handle} = $cid;
        }

        $self->{debug} && print "write chunk($seq) for id($id) of length(", length($chunk), ") at offset ($offset)\n";

        $db->SQL_ExecQuery( $cid, $id, $seq, $chunk )
            or $db->SQL_Error() && print( "ID: $id\nSEQ: $seq\nCHUNKSIZE: " . length($chunk) . "\n" ) && die;
        $seq++;
    }
}

=begin
Begin-Doc
Name: list_ids
Type: method
Description: returns list of ids in table
Syntax: @list = $obj->list_ids();
End-Doc
=cut

sub list_ids {
    my $self     = shift;
    my $table    = $self->{table};
    my $id_field = $self->{id_field};
    my $db       = $self->{db};

    my ( $id, @res );

    my $qry = "select distinct $id_field from $table order by $id_field";
    my $cid = $db->SQL_OpenQuery($qry) || $db->SQL_Error($qry) && return ();
    while ( ($id) = $db->SQL_FetchRow($cid) ) {
        push( @res, $id );
    }
    $db->SQL_CloseQuery($cid);
    return @res;
}

1;
