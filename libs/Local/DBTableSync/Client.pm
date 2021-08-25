#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
# This module contains portions copyright Curators of the University of Missouri.
#

# Begin-Doc
# Name: Local::DBTableSync::Client
# Type: module
# Description: base object/wrapper around all the db client functions that Local::DBTableSync uses
# End-Doc

package Local::DBTableSync::Client;
use strict;
use Local::DBTableSync::Client::MySQLObject;
use Local::DBTableSync::Client::OracleObject;
use Local::DBTableSync::Client::PostgreSQLObject;
use Local::MySQLObject;
use Local::OracleObject;
use Local::PostgreSQLObject;
use Text::CSV;

use constant { MAX_PENDING => 500 };

# Begin-Doc
# Name: new
# Type: method
# Description: creates object
# End-Doc
sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;
    my $tmp   = {};

    if ( ref( $opts{db} ) eq "HASH" && $opts{type} eq "dest" ) {
        $tmp->{read_db}  = $opts{db}{read};
        $tmp->{write_db} = $opts{db}{write};
        $tmp->{sep_db}   = 1;
    }
    else {
        $tmp->{read_db}  = $opts{db};
        $tmp->{write_db} = $opts{db};
        $tmp->{sep_db}   = 0;
    }

    my @params = qw{
        table   alias where       args
        dry_run force max_deletes max_inserts
        no_dups debug unique_keys ukey_sort
    };

    @{$tmp}{@params} = @opts{@params};
    $tmp->{error} = undef;

    $tmp->{type} = "source";
    if ( defined $opts{type} && $opts{type} =~ /^(source|dest)$/ ) {
        $tmp->{type} = $opts{type};
    }

    $tmp->{excl_cols} = {};
    foreach my $col ( split( /[\s,;]+/, $opts{excl_cols} ) ) {
        $tmp->{excl_cols}->{ lc $col } = 1;
    }

    $tmp->{mask_cols} = {};
    foreach my $col ( split( /[\s,;]+/, $opts{mask_cols} ) ) {
        my ( $cname, $val ) = split( /:/, $col );
        $tmp->{mask_cols}->{ lc $cname } = $val;
    }

    $tmp->{pending}         = 0;
    $tmp->{commits}         = 0;
    $tmp->{inserts}         = 0;
    $tmp->{deletes}         = 0;
    $tmp->{hit_max_inserts} = 0;
    $tmp->{hit_max_deletes} = 0;

    return bless $tmp, $class;
}

# Begin-Doc
# Name: init
# Type: method
# Description: kicks off initialization process for client module
# Returns: 1 on success, undef on error
# End-Doc
sub init {
    my $self = shift;

    #
    # Retrieve schema information
    #
    $self->_dprint( "Starting schema analysis for: " . $self->{table} );
    my $qry = "select * from " . $self->{table} . " where 1=0";
    if ( $self->{where} ) {
        $qry = "select * from " . $self->{table} . " where $self->{where} and 1=0";
    }
    my $cid = $self->{read_db}->SQL_OpenQuery( $qry, @{ $self->{args} } );

    if ( !$cid ) {
        $self->{error}
            = ref($self)
            . "::init - describe schema failed ("
            . $self->{table} . "): "
            . $self->{read_db}->SQL_ErrorString();
        return undef;
    }

    $self->{colinfo} = { $self->{read_db}->SQL_ColumnInfo($cid) };

    $self->_build_coltypes() || return undef;
    $self->_build_collists() || return undef;
    $self->_build_queries()  || return undef;

    #
    # Turn off autocommit if we're going to do any updates
    #
    if ( $self->{type} eq "dest" && !$self->{dry_run} ) {
        $self->{write_db}->SQL_AutoCommit(0);
    }
    return 1;
}

# Begin-Doc
# Name: close_queries
# Type: method
# Description: cleans up all open queries and commits any pending changes
# Returns: 1 on success, undef on error
# End-Doc
sub close_queries {
    my $self = shift;

    if ( !defined( $self->{queries} ) ) {
        $self->{error} = ref($self) . "::close_queries - no queries defined";
        return undef;
    }

    if ( $self->{pending} && $self->{type} eq "dest" && !$self->{dry_run} && !$self->{error} ) {
        $self->_dprint("pending changes, issuing commit.");
        if ( !$self->{write_db}->SQL_Commit() ) {
            $self->{error}
                = ref($self) . "::close_queries - unable to commit - " . $self->{write_db}->SQL_ErrorString();
            return undef;
        }
        $self->{commits}++;
    }

    foreach my $action (qw/select insert delete/) {
        if ( defined $self->{queries}->{$action} && defined $self->{queries}->{$action}->{cid} ) {
            my $db = $self->{queries}->{$action}->{db};
            $db->SQL_CloseQuery( $self->{queries}->{$action}->{cid} );
            delete $self->{queries}->{$action};
        }
    }

    #
    # handle unique deletes a little differently as there may
    # be multiple delete cids allocated
    #
    if ( defined $self->{queries}->{delete_uniq} && ref( $self->{queries}->{delete_uniq} ) eq "ARRAY" ) {
        foreach my $qryref ( @{ $self->{queries}->{delete_uniq} } ) {
            my $db  = $qryref->{db};
            my $cid = $qryref->{cid};
            $db->SQL_CloseQuery($cid);
        }
        delete $self->{queries}->{delete_uniq};
    }

    if ( $self->{type} eq "dest" ) {
        $self->{write_db}->SQL_AutoCommit(1);
    }

    return 1;
}

# Begin-Doc
# Name: colinfo
# Type: method
# Description: retrieves column info for the given table/query
# End-Doc
sub colinfo {
    my $self = shift;
    return $self->{colinfo};
}

# Begin-Doc
# Name: colnames
# Type: method
# Description: returns the column names for the given table/query
# End-Doc
sub colnames {
    my $self = shift;
    return $self->{colnames};
}

# Begin-Doc
# Name: coltypes
# Type: method
# Description: returns the column types for the given table/query
# End-Doc
sub coltypes {
    my $self = shift;
    if ( !$self->{coltypes} ) {
        my @cols = @{ $self->colnames() };
        @{ $self->{coltypes} } = @{ $self->{types} }{@cols};
    }
    return $self->{coltypes};
}

# Begin-Doc
# Name: commits
# Type: method
# Description: returns the number of commits submitted to the database
# End-Doc
sub commits {
    my $self = shift;
    return $self->{commits};
}

# Begin-Doc
# Name: skipcols
# Type: method
# Description: returns a hashref of all columns that are skipped
# End_doc
sub skipcols {
    my $self = shift;
    return $self->{skipcols};
}

# Begin-Doc
# Name: inserts
# Type: method
# Description: returns the number of rows inserted
# End-Doc
sub inserts {
    my $self = shift;
    return $self->{inserts};
}

# Begin-Doc
# Name: deletes
# Type: method
# Description: returns the number of rows deleted
# End-Doc
sub deletes {
    my $self = shift;
    return $self->{deletes};
}

# Begin-Doc
# Name: _build_coltypes
# Type: method
# Description: analyzes the schema of the table/query in question to determine each column type
#              this information is used later in the comparison routine in Local::DBTableSync
# Comments: non-implemented stub - each database specific implementation slightly differs
# End-Doc
sub _build_coltypes {
    my $self = shift;
    $self->{error} = ref($self) . "::_build_coltypes - method not implemented";
    return undef;
}


# Begin-Doc
# Name: _build_collists
# Type: method
# Description: builds the following column lists
#       $self->{select_cols} - arrayref (ordered) list of database specific column statements - used to build select statements
#       $self->{colnames}    - arrayref of lowercase column names
#       $self->{sort_cols}   - arrayref order in which results of select statement will be ordered - ensure NULLs first
# End-Doc
sub _build_collists {
    my $self = shift;

    $self->{select_cols} = [];
    $self->{insert_cols} = [];
    $self->{colnames}    = [];
    $self->{sort_cols}   = [];

    my @lower_cols       = map { lc $_ } @{ $self->{colinfo}->{colnames} };
    my $default_ordering = 1;

    if ( $self->{ukey_sort} ) {
        my $ukey;
        $self->_dprint("Checking for unique key based sort.\n");

        if ( ref( $self->{ukey_sort} ) eq "ARRAY" && scalar @{ $self->{ukey_sort} } ) {
            $self->_dprint( "Elected to use supplied key as sort: " . join( ", ", @{ $self->{ukey_sort} } ) . "\n" );
            $ukey             = $self->{ukey_sort};
            $default_ordering = 0;
        }
        else {

            #
            # If we have any unique keys, we can sort on the shortest key for the fastest possible sort
            # For now, just grab the first one in the list
            #
            foreach my $keys ( @{ $self->{unique_keys} } ) {
                next unless scalar @{$keys};
                $self->_dprint( "Elected to use unique key as sort: " . join( ", ", @{$keys} ) . "\n" );
                $ukey             = $keys;
                $default_ordering = 0;
                last;
            }
        }

        #
        # Now that we have a unique key selected for sort ordering
        # build up colnames ordering to maintain lock-step ordering/comparator
        #
        if ( !$default_ordering && $ukey ) {
            my $ulen     = scalar @{$ukey};
            my $rank     = $ulen + 1;
            my %colranks = map { $_ => $rank++; } @lower_cols;

            $rank = 0;
            foreach my $col ( map { lc $_ } @{$ukey} ) {
                $colranks{$col} = $rank++;
                push( @{ $self->{sort_cols} }, $col );
            }

            @lower_cols = sort { $colranks{$a} <=> $colranks{$b} } @lower_cols;
        }
    }

    foreach my $col (@lower_cols) {
        unless ( $self->{skipcols}->{$col} ) {
            if ( exists( $self->{mask_cols}->{$col} ) && $self->{type} eq "source" ) {
                my $tcol = $self->{read_db}->SQL_QuoteString( $self->{mask_cols}->{$col} ) . " " . $col;
                push( @{ $self->{select_cols} }, $tcol );
            }
            else {
                push( @{ $self->{select_cols} }, $col );
            }
            push( @{ $self->{colnames} },    $col );
            push( @{ $self->{insert_cols} }, $col );
        }

        unless ( $self->{skipcols}->{$col} || $self->{skiplong}->{$col} ) {
            if ($default_ordering) {
                push( @{ $self->{sort_cols} }, $col );
            }
        }
    }

    return 1;
}

# Begin-Doc
# Name: _build_queries
# Type: method
# Description: builds the queries/cids necessary to perform client operations (select, insert, delete)
# End-Doc
sub _build_queries {
    my $self = shift;

    $self->_build_select() || return undef;

    # only build insert/delete queries if
    # handling dest object
    if ( $self->{type} eq "dest" ) {
        $self->_build_insert() || return undef;
        $self->_build_delete() || return undef;
    }
    return 1;
}

# Begin-Doc
# Name: _build_select
# Type: method
# Description: builds internal select query for later use
# End-Doc
sub _build_select {
    my $self        = shift;
    my $select_cols = join( ",", @{ $self->{select_cols} } );
    my $sort_cols   = join( ",", @{ $self->{sort_cols} } );
    my $table       = $self->{table};
    my $alias       = $self->{alias};
    my $where       = $self->{where};
    my $qry         = "select";

    if ( $self->{no_dups} ) {
        $qry .= " distinct";
    }

    $qry .= " ${select_cols} from ${table} ${alias}";
    if ($where) {
        $qry .= " where ${where}";
    }
    $qry .= " order by ${sort_cols}";

    $self->{queries}->{select}->{qry} = $qry;

    if ( $self->{args} ) {
        $self->{queries}->{select}->{args} = $self->{args};
    }

    return 1;
}

# Begin-Doc
# Name: _build_insert
# Type: method
# Description: builds internal insert query for later use
# End-Doc
sub _build_insert {
    my $self        = shift;
    my $insert_cols = join( ",", @{ $self->{insert_cols} } );
    my $table       = $self->{table};
    my $args        = join( ",", ("?") x scalar @{ $self->colnames() } );
    my $qry         = "insert into ${table} (${insert_cols}) values (${args})";

    $self->_dprint("\nOpening insert query: ${qry}");
    my $cid = $self->{write_db}->SQL_OpenBoundQuery($qry);
    unless ($cid) {
        $self->{error} = ref($self) . "::_build_insert - failed to open insert query";
        return undef;
    }

    $self->{queries}->{insert} = {
        cid    => $cid,
        qry    => $qry,
        db     => $self->{write_db},
        fields => $self->colnames()
    };

    return 1;
}

# Begin-Doc
# Name: _build_delete
# Type: method
# Description: builds internal delete queries for later use
# Comments: non-implemented stub
# End-Doc
sub _build_delete {
    my $self = shift;
    $self->{error} = ref($self) . "::_build_delete - method not implemented";
    return undef;
}

# Begin-Doc
# Name: error
# Type: method
# Description: returns any internal error message (used in conjuction with methods that may return undef on error)
# End-Doc
sub error {
    my $self = shift;
    return $self->{error};
}

# Begin-Doc
# Name: _dprint
# Type: method
# Description: prints message only if debugging is enabled
# End-Doc
sub _dprint {
    my $self = shift;
    if ( $self->{debug} ) {
        print @_, "\n";
    }
}

# Begin-Doc
# Name: dump_colinfo
# Type: method
# Description: Generates schema information for given table/query and returns as string
#              used later for schema comparisons
# End-Doc
sub dump_colinfo {
    my $self = shift;
    my $res  = "Column Count(" . $self->{colinfo}->{numcols} . ")\n";
    for ( my $i = 0; $i < $self->{colinfo}->{numcols}; $i++ ) {
        my $name  = uc $self->{colinfo}->{colnames}->[$i];
        my $type  = $self->{colinfo}->{coltypes}->[$i];
        my $prec  = $self->{colinfo}->{precision}->[$i];
        my $scale = $self->{colinfo}->{scale}->[$i];
        $res .= "  $name: Type($type)";

        if ($prec) {
            $res .= "  Prec($prec)";
        }
        if ($scale) {
            $res .= "  Scale($scale)";
        }
        $res .= "\n";
    }

    return $res;
}

# Begin-Doc
# Name: _open_select
# Type: method
# Description: opens select query
# Returns: 1 on success, undef on error
# End-Doc
sub _open_select {
    my $self = shift;
    my $type = $self->{type};
    my $db   = $self->{read_db};
    my $qry  = $self->{queries}->{select}->{qry};
    my $args = $self->{queries}->{select}->{args};
    my $cid = $db->SQL_OpenQuery( $qry, @{$args} );

    $self->_dprint("\nOpening select query (${type}): ${qry}");

    if ( !$cid ) {
        $self->{error} = ref($self) . "::_open_select - unable to open select query - " . $db->SQL_ErrorString();
        return undef;
    }

    $self->{queries}->{select}->{cid} = $cid;
    $self->{queries}->{select}->{db}  = $db;
    return 1;
}

# Begin-Doc
# Name: fetch_row
# Type: method
# Description: fetches next row in select statement from database
# Returns: arrayref of row data, undef if no more row data, and undef on error
# Comments: check $obj->error() on undef return from this function
# End-Doc
sub fetch_row {
    my $self = shift;

    if ( !$self->{queries}->{select}->{cid} ) {
        if ( !$self->_open_select() ) {
            return undef;
        }
    }

    my $cid = $self->{queries}->{select}->{cid};
    my $db  = $self->{queries}->{select}->{db};
    my $row = $db->SQL_FetchRowRef($cid);
    my $err = $db->SQL_ErrorCode();

    if ( !defined($row) && $err ) {
        $self->{error} = ref($self) . "::fetch_row - ${err} - " . $db->SQL_ErrorString();
    }

    return $row;
}

# Begin-Doc
# Name: insert_row
# Type: method
# Description: inserts row into database
# Syntax: $obj->insert_row( @row_values );
# Returns: 1 on success, undef on error
# End-Doc
sub insert_row {
    my $self = shift;
    my %rowdata;
    my @colnames = @{ $self->colnames() };

    @rowdata{@colnames} = @_;

    if ( !$self->{force} && $self->{max_inserts} && $self->{inserts} >= $self->{max_inserts} ) {
        if ( !$self->{hit_max_inserts} ) {
            my $max = $self->{max_inserts};
            $self->{hit_max_inserts} = 1;

            $self->_dprint("max inserts reached (${max})");
        }

        if ( !$self->{dry_run} ) {
            $self->_dprint("rolling back updates.");
            $self->{write_db}->SQL_RollBack();
        }

        $self->{error} = ref($self) . "::insert_row - max inserts reached";
        return undef;
    }

    if ( !defined $self->{queries}->{insert} ) {
        $self->{error} = ref($self) . "::insert_row - insert query not built";
        return undef;
    }

    my $cid    = $self->{queries}->{insert}->{cid};
    my $db     = $self->{queries}->{insert}->{db};
    my @fields = @{ $self->{queries}->{insert}->{fields} };

    unless ( $db->SQL_ExecQuery( $cid, @rowdata{@fields} ) ) {
        $self->{error} = ref($self) . "::insert_row - unable to insert row - " . $db->SQL_ErrorString();
        return undef;
    }

    $self->{pending}++;
    $self->{inserts}++;
    return 1;
}

# Begin-Doc
# Name: delete_row
# Type: method
# Description: deletes supplied row from database
# Syntax: $obj->delete_row( @row_values );
# Returns: number of rows deleted on success, undef on error
# End-Doc
sub delete_row {
    my $self = shift;
    my %rowdata;
    my $cnt;
    my @colnames = @{ $self->colnames() };

    @rowdata{@colnames} = @_;

    if ( !$self->{force} && $self->{max_deletes} && $self->{deletes} >= $self->{max_deletes} ) {
        if ( !$self->{hit_max_deletes} ) {
            my $max = $self->{max_deletes};
            $self->{hit_max_deletes} = 1;

            $self->_dprint("max deletes reached (${max})");
        }

        if ( !$self->{dry_run} ) {
            $self->_dprint("rolling back updates.");
            $self->{write_db}->SQL_RollBack();
        }

        $self->{error} = ref($self) . "::delete_row - max deletes reached";
        return undef;
    }

    if ( !defined $self->{queries}->{delete} ) {
        $self->{error} = ref($self) . "::delete_row - delete query not built";
        return undef;
    }

    my $db     = $self->{queries}->{delete}->{db};
    my $cid    = $self->{queries}->{delete}->{cid};
    my @fields = @{ $self->{queries}->{delete}->{fields} };

    unless ( $db->SQL_ExecQuery( $cid, @rowdata{@fields} ) ) {
        $self->{error} = ref($self) . "::delete_row - unable to delete row - " . $db->SQL_ErrorString();
        return undef;
    }
    $cnt = $db->SQL_RowCount();

    $self->{pending}++;
    $self->{deletes}++;
    return $cnt;
}

# Begin-Doc
# Name: delete_uniq
# Type: method
# Description: deletes any rows from database on any matching unique keys
# Syntax: $obj->delete_uniq( @row_values );
# Returns: number of rows deleted on success, undef on error
# End-Doc
sub delete_uniq {
    my $self     = shift;
    my $cnt      = 0;
    my @colnames = @{ $self->colnames() };
    my %rowdata;

    @rowdata{@colnames} = @_;

    foreach my $qryref ( @{ $self->{queries}->{delete_uniq} } ) {
        my $db     = $qryref->{db};
        my $cid    = $qryref->{cid};
        my @fields = @{ $qryref->{fields} };

        unless ( $db->SQL_ExecQuery( $cid, @rowdata{@fields} ) ) {
            $self->{error} = ref($self) . "::delete_uniq - unable to delete row - " . $db->SQL_ErrorString();
            return undef;
        }
        $cnt += $db->SQL_RowCount();
    }

    if ($cnt) {
        $self->{pending}++;
    }

    return $cnt;
}

# Begin-Doc
# Name: row_count
# Type: method
# Description: returns number of rows in given table/query
# Syntax: $obj->row_count( $database_key );
#   $database_key - (optional) database selector (either read_db or write_db)
# End-Doc
sub row_count {
    my $self  = shift;
    my $which = shift || "write_db";
    my $db    = $self->{$which};

    if ( $which !~ m/^(read|write)_db$/ || !defined($db) ) {
        $self->{error} = ref($self) . "::row_count - unknown db key (${which})";
        return undef;
    }
    my $table = $self->{table};
    my $alias = $self->{alias};
    my $where = $self->{where};

    my $qry = "select count(*) from ${table} ${alias}";
    if ($where) {
        $qry .= "where ${where}";
    }

    my ($cnt) = $db->SQL_DoQuery( $qry, @{ $self->{args} } );

    if ( !defined($cnt) ) {
        $self->{error} = ref($self) . "::row_count - unable to retreive row count: " . $db->SQL_ErrorString();
        return undef;
    }
    return $cnt;
}

# Begin-Doc
# Name: dump_table
# Type: method
# Description: dumps (csv) contents of table/query to supplied file
# Syntax: $obj->dump_table( $filename, $database_key );
#   $filename     - self explanatory
#   $database_key - (optional) database selector (either read_db or write_db)
# Returns: 1 on success, undef on error
# End-Doc
sub dump_table {
    my $self  = shift;
    my $file  = shift;
    my $which = shift || "write_db";
    my $db    = $self->{$which};

    if ( $which !~ m/^(read|write)_db$/ || !defined($db) ) {
        $self->{error} = ref($self) . "::dump_table - unknown db key (${which})";
        return undef;
    }

    my $qry = $self->{queries}->{select}->{qry};
    if ( !$qry ) {
        $self->{error} = ref($self) . "::dump_table - no select query built";
        return undef;
    }

    my $csv = new Text::CSV( { binary => 1 } );
    open( my $out, ">${file}" ) || die;

    my $cid = $db->SQL_OpenQuery( $qry, @{ $self->{args} } );
    while ( my @tmp = $db->SQL_FetchRow($cid) ) {
        $csv->combine(@tmp);
        print $out $csv->string(), "\n";
    }
    $db->SQL_CloseQuery($cid);
    close($out);
    return 1;
}

# Begin-Doc
# Name: check_pending
# Type: method
# Description: if in force mode, executes any pending commits (once in excess of MAX_PENDING commits)
# Returns: 1 on success undef on failure
# End-Doc
sub check_pending {
    my $self = shift;

    if ( $self->{type} eq "dest" && $self->{force} && $self->{pending} > MAX_PENDING ) {
        $self->_dprint("max pending updates reached, committing.");
        if ( !$self->{dry_run} ) {
            if ( !$self->{write_db}->SQL_Commit() ) {
                $self->{error}
                    = ref($self) . "::check_pending - unable to commit - " . $self->{write_db}->SQL_ErrorString();
                return undef;
            }
        }
        $self->{pending} = 0;
        $self->{commits}++;
    }
    return 1;
}

# Begin-Doc
# Name: roll_back
# Type: method
# Description: rolls back any pending commits
# End-Doc
sub roll_back {
    my $self = shift;
    if ( $self->{type} ne "dest" ) {
        return;
    }

    $self->{write_db}->SQL_RollBack();
}

1;
