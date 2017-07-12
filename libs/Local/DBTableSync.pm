
=pod

Begin-Doc
Name: Local::DBTableSync
Type: module
Description: object to manage sychronizing content of two database tables

Example:

$srcdb = new Local::OracleObject;
$srcdb->SQL_OpenDatabase("srcdb");

$destdb = new Local::OracleObject;
$destdb->SQL_OpenDatabase("destdb");

$sync = new Local::DBTableSync(
    debug => 0,
);

my %res = $sync->SyncTables(
    source_db => $srcdb,
    dest_db => $destdb,
    # excl_cols => $col_list,
    # mask_cols => $col_list_with_opt_colon_value,
    source_table => "table_name_on_test",
    dest_table => "table_name_on_dev",
    max_inserts => 10000,

    # Example table: position, department, user
    # Unique index on position,department and a unique index on user (one job per userid and per position nbr)
    # if we had a composite key field, include it here as well
    # if no unique constraints exist, this attribute can be left out
    unique_keys => [
        ["position","department"],
        ["user"],
    ],
);

Note that module has only been actively tested with OracleObject at this time.
 
End-Doc

=cut

package Local::DBTableSync;
require Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use JSON;
use Local::UsageLogger;
use Time::HiRes qw(time);
use Text::CSV;

BEGIN {
    &LogAPIUsage();
}

@ISA    = qw(Exporter);
@EXPORT = qw();

# Begin-Doc
# Name: new
# Type: function
# Description: Creates object
# Syntax: $sync = new Local::DBTableSync(%opts)
# Comments: options are:
#    compare_schemas: error if SQL_ColumnInfo returns different information from src/dest
#    dry_run: just return status of what would have been done, no updates
#    force: force update regardless of any set limits
#    max_deletes: set maximum number of deletes that will be issued
#    max_inserts: set maximum number of inserts that will be issued
#    no_dups: hint that there will be NO 100% duplicated records in destination
#    debug: enable/disable debuging (1/0)
#    check_empty_source: check for empty source table and fail sync if empty
# End-Doc
sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;

    my $tmp = {};

    &LogAPIUsage();

    $tmp->{debug} = $opts{debug};
    $tmp->{error} = undef;

    $tmp->{compare_schemas} = 1;
    if ( exists( $opts{compare_schemas} ) ) {
        $tmp->{compare_schemas} = $opts{compare_schemas};
    }

    $tmp->{dry_run} = 0;
    if ( exists( $opts{dry_run} ) ) {
        $tmp->{dry_run} = $opts{dry_run};
    }

    $tmp->{force} = 0;
    if ( exists( $opts{force} ) ) {
        $tmp->{force} = $opts{force};
    }

    $tmp->{max_deletes} = undef;
    if ( exists( $opts{max_deletes} ) ) {
        $tmp->{max_deletes} = $opts{max_deletes};
    }

    $tmp->{max_inserts} = undef;
    if ( exists( $opts{max_inserts} ) ) {
        $tmp->{max_inserts} = $opts{max_inserts};
    }

    $tmp->{no_dups} = undef;
    if ( exists( $opts{no_dups} ) ) {
        $tmp->{no_dups} = $opts{no_dups};
    }

    $tmp->{check_empty_source} = undef;
    if ( exists( $opts{check_empty_source} ) ) {
        $tmp->{check_empty_source} = $opts{check_empty_source};
    }

    $tmp->{dumpfile} = undef;
    if ( exists( $opts{dumpfile} ) ) {
        $tmp->{dumpfile} = $opts{dumpfile};
    }

    return bless $tmp, $class;
}

# Begin-Doc
# Name: error
# Type: method
# Description: returns any error message that may be set in object
# Syntax: $err = $obj->error();
# Comments: this should be checked after any operation to determine success/failure
# End-Doc
sub error {
    my $self = shift;
    return $self->{error};
}

# Begin-Doc
# Name: _debug
# Type: method
# Description: returns if debugging is enabled
# Syntax: $status = $obj->_debug();
# Access: internal
# End-Doc
sub _debug {
    my $self = shift;
    return $self->{debug};
}

# Begin-Doc
# Name: _dprint
# Type: method
# Description: prints message only if debugging is enabled
# Syntax: $obj->_dprint(@print_args);
# Access: internal
# End-Doc
sub _dprint {
    my $self = shift;
    if ( $self->{debug} ) {
        print @_;
        print "\n";
    }
}

# Begin-Doc
# Name: _dprintrow
# Type: method
# Description: prints message only if debugging is enabled and at level 2
# Syntax: $obj->_dprintrow($label, $rowref);
# Access: internal
# End-Doc
sub _dprintrow {
    my $self   = shift;
    my $label  = shift;
    my $rowref = shift;

    if ( $self->{debug} > 1 ) {
        if ($rowref) {
            print "$label: [ ", join( " | ", @{$rowref} ), " ]\n";
        }
        else {
            print "$label: NULL ROW\n";
        }
    }
}

# Begin-Doc
# Name: _dprintrowall
# Type: method
# Description: prints message only if debugging is enabled and at level 3
# Syntax: $obj->_dprintrowall($label, $rowref);
# Access: internal
# End-Doc
sub _dprintrowall {
    my $self   = shift;
    my $label  = shift;
    my $rowref = shift;

    if ( $self->{debug} > 2 ) {
        if ($rowref) {
            print "$label: ", join( " | ", @{$rowref} ), "\n";
        }
        else {
            print "$label: NULL ROW\n";
        }
    }
}

# Begin-Doc
# Name: SyncTables
# Type: function
# Description: routine that is called to sync tables, separated to allow for easy multipass
# Syntax: %res = $obj->SyncTables(%opts)
# Comments: This takes the same arguments as the 'new' routine, but also takes some additional
# required options.
#
#    source_db => source db object
#    dest_db => dest db object
#    source_table => source table/view name
#    dest_table => dest table name
#    excl_cols => column names to exclude from sync
#    mask_cols => column names and optional values to mask during sync
#    unique_keys => optional array ref, containing array refs, each of which is a list of column names
#       that make up a unique index on the table. If the table has unique indexes, it is strongly
#       recommended that this be included
#    source_where => optional string, appended as where clause on select of rows from source table
#    dest_where => optional string, appended as where clause on select of rows from destination table
#    source_alias => alias for primary source table, can be useful in where clause
#    dest_alias => alias for primary dest table, can be useful in where clause
#
#    pre_setup_check => callback sub reference, called prior to schema analysis
#    pre_select_check => callback sub reference, called prior to opening of select/insert queries
#    post_sync_check => callback sub reference, called prior to final commit, after all inserts/deletes
#    post_commit_check => callback sub reference, called prior to final commit, after final commit
#      Optional: only called if reference is provided
#      These routines are called with the same parameters that were passed to SyncTables routine,
#      and should return undef. If anything other than undef is returned, it will be treated as
#      a fatal error.
#
#    check_empty_source => check for empty source table and fail if table is empty
#
# Comments: this expects case insensitive column and table names, if you've done something different
# you need to stop doing that, it's evil. Additionally, this will skip any *LOB columns quietly. Nulls
# will be inserted in the destination for any LOB columns.
#
# End-Doc
sub SyncTables {
    my $self = shift;
    my %opts = @_;

    my ( $source_db, $dest_db, $source_table, $dest_table );
    my ( $source_where, $dest_where );
    my ( $source_alias, $dest_alias );
    my ( $qry,          $cid );
    my ( $no_dups,      $ignore_row_count );
    $self->{error} = undef;

    &LogAPIUsage();

    $self->{start_time} = time;
    my @tmp_times = times;
    $self->{start_user_cpu}   = $tmp_times[0];
    $self->{start_system_cpu} = $tmp_times[1];

    #
    # Determine config parms/limits, override on this request if set
    #
    my $compare_schemas = $self->{compare_schemas};
    if ( exists( $opts{compare_schemas} ) ) {
        $compare_schemas = $opts{compare_schemas};
    }

    my $dry_run = $self->{dry_run};
    if ( exists( $opts{dry_run} ) ) {
        $dry_run = $opts{dry_run};
    }

    my $force = $self->{force};
    if ( exists( $opts{force} ) ) {
        $force = $opts{force};
    }

    my $max_deletes = $self->{max_deletes};
    if ( exists( $opts{max_deletes} ) ) {
        $max_deletes = $opts{max_deletes};
    }

    my $max_inserts = $self->{max_inserts};
    if ( exists( $opts{max_inserts} ) ) {
        $max_inserts = $opts{max_inserts};
    }

    my $check_empty_source = $self->{check_empty_source};
    if ( exists( $opts{check_empty_source} ) ) {
        $check_empty_source = $opts{check_empty_source};
    }

    if ( exists( $opts{source_where} ) ) {
        $source_where = $opts{source_where};
    }
    if ( exists( $opts{dest_where} ) ) {
        $dest_where = $opts{dest_where};
    }

    if ( exists( $opts{no_dups} ) ) {
        $no_dups = $opts{no_dups};
    }

    if ( exists( $opts{ignore_row_count} ) ) {
        $ignore_row_count = $opts{ignore_row_count};
    }

    my $dumpfile;
    if ( exists( $opts{dumpfile} ) ) {
        $dumpfile = $opts{dumpfile};
    }

    # Columns to skip
    my %excl_cols = ();
    foreach my $col ( split( /[\s,;]+/, $opts{excl_cols} ) ) {
        $excl_cols{ lc $col } = 1;
    }

    my %mask_cols = ();
    foreach my $col ( split( /[\s,;]+/, $opts{mask_cols} ) ) {
        my ( $cname, $val ) = split( /:/, $col );
        $mask_cols{ lc $cname } = $val;
    }

    #
    # Thought about setting defaults, but didn't like idea of potentially overwriting
    # something due to "defaults"
    #
    $source_db = $opts{source_db}
        || return ( error => "missing source_db", status => "failed" );
    $dest_db = $opts{dest_db}
        || return ( error => "missing dest_db", status => "failed" );

    $source_table = $opts{source_table}
        || return ( error => "missing source_table", status => "failed" );
    $dest_table = $opts{dest_table}
        || return ( error => "missing dest_table", status => "failed" );

    $source_alias = $opts{source_alias};
    $dest_alias   = $opts{dest_alias};

    $self->_dprint("starting setup of sync of $source_table to $dest_table");

    #
    # Run the pre_setup_check callback
    #
    if ( $opts{pre_setup_check} ) {
        my $res = $opts{pre_setup_check}->(%opts);
        if ($res) {
            return (
                error  => "pre_setup_check failed: $res",
                status => "failed"
            );
        }
    }

    #
    # If Oracle, set appropriate default date formats
    #
    if ( ref($source_db) =~ /Oracle/ ) {
        my $date_qry = "alter session set NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'";
        unless ( $source_db->SQL_ExecQuery($date_qry) ) {
            $self->{error} = "set of source nls date format failed: " . $source_db->SQL_ErrorString();
            return ( error => $self->{error}, status => "failed" );
        }

        my $ts_qry = "alter session set NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF'";
        unless ( $source_db->SQL_ExecQuery($ts_qry) ) {
            $self->{error} = "set of source nls date format failed: " . $source_db->SQL_ErrorString();
            return ( error => $self->{error}, status => "failed" );
        }

        $source_db->dbhandle->{ChopBlanks} = 0;

        # From DBD::Oracle docs and source
        # 96: ORA_CHAR
        # Don't strip trailing spaces and allow embedded \0.  Force 'blank-padded comparison semantics
        $source_db->dbhandle->{ora_ph_type} = 96;
    }
    if ( ref($dest_db) =~ /Oracle/ ) {
        my $date_qry = "alter session set NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'";

        unless ( $dest_db->SQL_ExecQuery($date_qry) ) {
            $self->{error} = "set of destination nls date format failed: " . $dest_db->SQL_ErrorString();
            return ( error => $self->{error}, status => "failed" );
        }

        my $ts_qry = "alter session set NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF'";

        unless ( $dest_db->SQL_ExecQuery($ts_qry) ) {
            $self->{error} = "set of destination nls timestamp format failed: " . $dest_db->SQL_ErrorString();
            return ( error => $self->{error}, status => "failed" );
        }

        $dest_db->dbhandle->{ChopBlanks} = 0;

        # See above
        $dest_db->dbhandle->{ora_ph_type} = 96;
    }

    #
    # Retrieve schema information for source
    #
    $self->_dprint("starting schema analysis for sync of $source_table to $dest_table");

    $qry = "select * from $source_table $source_alias where 1=0";
    if ($source_where) {
        $qry .= " and $source_where";
    }
    $cid = $source_db->SQL_OpenQuery($qry);
    if ( !$cid ) {
        $self->{error} = "describe schema from source failed: " . $source_db->SQL_ErrorString();
        return ( error => $self->{error}, status => "failed" );
    }
    my %source_colinfo = $source_db->SQL_ColumnInfo($cid);
    $source_db->SQL_CloseQuery($cid);

    my $source_orig_dump = $self->dump_colinfo( \%source_colinfo );
    $self->_dprint("Unfiltered Source Schema Info:\n\n$source_orig_dump\n");

    #
    # Retrieve schema information for dest
    #
    $qry = "select * from $dest_table $dest_alias where 1=0";
    if ($dest_where) {
        $qry .= " and $dest_where";
    }
    $cid = $dest_db->SQL_OpenQuery($qry);
    if ( !$cid ) {
        $self->{error} = "describe schema from dest failed: " . $dest_db->SQL_ErrorString();
        return ( error => $self->{error}, status => "failed" );
    }
    my %dest_colinfo = $dest_db->SQL_ColumnInfo($cid);
    $dest_db->SQL_CloseQuery($cid);

    my $dest_orig_dump = $self->dump_colinfo( \%dest_colinfo );
    $self->_dprint("Unfiltered Dest Schema Info:\n\n$dest_orig_dump\n");

    #
    # Build listing of column comparison types (string or numeric)
    #
    my @coltypes = ();
    my %skipcols = ();
    my %skiplong = ();
    {

        # This is using some internals of our DB wrappers, but can't be helped if
        # we want to get column type information in any reasonable fashion
        my $dbh = $source_db->dbhandle;
        my $tia = $dbh->type_info_all;

        my %sql_type_to_name = ();
        foreach my $entry ( @{$tia} ) {

            if ( ref($entry) eq "ARRAY" ) {
                my ( $name, $itype ) = @{$entry};

                next if ( $sql_type_to_name{$itype} );
                $sql_type_to_name{$itype} = $name;
            }
        }

        my @scoltypes = @{ $source_colinfo{coltypes} };
        my @dcoltypes = @{ $dest_colinfo{coltypes} };
        my @scolnames = @{ $source_colinfo{colnames} };
        my @dcolnames = @{ $dest_colinfo{colnames} };

        my $dindex = 0;
        for ( my $sindex = 0; $sindex <= $#scoltypes; $sindex++ ) {
            my $coltype  = $scoltypes[$sindex];
            my $dcoltype = $dcoltypes[$dindex];
            my $colname  = $scolnames[$sindex];
            my $dcolname = $dcolnames[$dindex];

            my $tname  = $sql_type_to_name{$coltype};
            my $dtname = $sql_type_to_name{$dcoltype};

            # Check for excluded columns
            if ( exists( $excl_cols{ lc $colname } ) ) {
                $self->_dprint("Checking type: $sindex / $colname / $coltype / $tname => Excluded\n");
                $skipcols{ lc $colname } = 1;
                next;
            }

            $self->_dprint(
                "Checking type: $sindex / $colname / $coltype / $tname => $dcolname / $dcoltype / $dtname\n");

            # type numbers are magic/from ODBC
            if ( exists( $mask_cols{ lc $colname } ) ) {
                push( @coltypes, "string" );
            }
            elsif ($tname =~ /CHAR/
                || $tname =~ /TIME/
                || $tname =~ /DATE/
                || $tname =~ /BIN/ )
            {
                push( @coltypes, "string" );
            }
            elsif ( $tname =~ /INTERVAL/ ) {
                push( @coltypes, "string" );
            }
            elsif ( $tname =~ /RAW/ ) {

                # can't handle LONG RAW right now
                push( @coltypes, "unknown" );
                $skipcols{ lc $colname } = 1;
            }
            elsif ( $tname =~ /LONG/ || $coltype == 40 ) {

                # not sure why 40 isn't in the types table though
                # 40 = CLOB
                # Yuck. I can handle longs
                if ( $dtname =~ /LONG/ || $dcoltype == 40 ) {
                    push( @coltypes, "string" );
                    $skiplong{ lc $colname } = 1;
                }
                else    # dest is different type or something else weird
                {
                    push( @coltypes, "unknown" );
                    $skipcols{ lc $colname } = 1;
                }
            }
            elsif ( $tname =~ /BFILE/ ) {
                push( @coltypes, "unknown" );
                $skipcols{ lc $colname } = 1;
            }
            elsif ($tname =~ /DEC/
                || $tname =~ /INT/
                || $tname =~ /NUM/
                || $tname =~ /DOUBLE/ )
            {
                push( @coltypes, "numeric" );
            }
            else {
                $self->{error} = "don't know how to compare column $colname (type $coltype [$tname])";
                return ( error => $self->{error}, status => "failed" );
            }

            $dindex++;
        }
    }

    #
    # Build column lists
    #
    my @source_cols;
    my @dest_cols;

    my @source_sort_cols;
    my @dest_sort_cols;

    my %masked_cols = ();
    foreach my $col ( @{ $source_colinfo{colnames} } ) {
        unless ( $skipcols{ lc $col } ) {
            if ( exists $mask_cols{ lc $col } ) {
                my $tcol = $source_db->SQL_QuoteString( $mask_cols{ lc $col } ) . " " . lc($col);
                push( @source_cols, $tcol );
                $masked_cols{$tcol} = 1;
                $masked_cols{ lc $col } = 1;
            }
            else {
                push( @source_cols, $col );
            }
        }
        unless ( $skipcols{ lc $col } || $skiplong{ lc $col } ) {
            push( @source_sort_cols, $col );
        }
    }
    foreach my $col ( @{ $dest_colinfo{colnames} } ) {
        unless ( $skipcols{ lc $col } ) {
            push( @dest_cols, $col );
        }
        unless ( $skipcols{ lc $col } || $skiplong{ lc $col } ) {
            push( @dest_sort_cols, $col );
        }
    }

    my $source_cols      = join( ", ", @source_cols );
    my $dest_cols        = join( ", ", @dest_cols );
    my $source_sort_cols = join( ", ", @source_sort_cols );
    my $dest_sort_cols   = join( ", ", @dest_sort_cols );
    my %have_source_cols = map { $_ => 1 } @source_cols;
    my %have_dest_cols   = map { $_ => 1 } @dest_cols;

    my $col_compare = "";

    foreach my $col (@source_cols) {
        if ( !$have_dest_cols{$col} && !exists $masked_cols{$col} ) {
            $col_compare .= "Column $col in source but not in destination.\n";
        }
    }
    foreach my $col (@dest_cols) {
        if ( !$have_source_cols{$col} && !exists $masked_cols{ lc $col } ) {
            $col_compare .= "Column $col in destination but not in source.\n";
        }
    }

    if ( $#source_cols != $#dest_cols ) {
        my $s_col = $#source_cols + 1;
        my $d_col = $#dest_cols + 1;

        my $msg = "Sync-Failure: mismatched column counts\n";
        $msg .= "Source has $s_col columns, destination has $d_col columns.\n\n";

        if ($col_compare) {
            $msg .= $col_compare . "\n";
            $msg .= "\n";
        }
        $msg .= "  Source Cols: $source_cols\n";
        $msg .= "  Dest Cols: $dest_cols\n";

        $self->_dprint($msg);
        $self->{error} = $msg;

        return ( error => $self->{error}, status => "failed" );
    }

    #
    # Build column number lists for unique key row deletion
    #
    my @unique_info = ();
    if ( $opts{unique_keys} ) {
        my %valid_cols = map { uc $_ => 1 } @dest_cols;

        foreach my $cref ( @{ $opts{unique_keys} } ) {
            next if ( ref($cref) ne "ARRAY" );
            my %col_names = ();

            foreach my $cname ( map { uc $_ } @{$cref} ) {
                if ( $valid_cols{$cname} ) {
                    $col_names{$cname} = 1;
                }
                else {
                    $self->{error} = "invalid column name for key ($cname)";
                    return ( error => $self->{error}, status => "failed" );
                }
            }

            push( @unique_info, { fields => {%col_names} } );
        }
    }

    #
    # Compare the schemas to make certain that they are identical, but only
    # if the compare_schema option is enabled.
    #

    if ($compare_schemas) {
        my $source_dump = $self->dump_colinfo( \%source_colinfo );
        my $dest_dump   = $self->dump_colinfo( \%dest_colinfo );

        # Short circuit check
        if ( $source_dump ne $dest_dump ) {
            my $msg = "";
            if ($col_compare) {
                $msg .= $col_compare . "\n";
            }

            my $dindex = 0;
            for ( my $sindex = 0; $sindex < $source_colinfo{numcols}; $sindex++ ) {
                my $sname  = $source_colinfo{colnames}->[$sindex];
                my $stype  = $source_colinfo{coltypes}->[$sindex];
                my $sprec  = $source_colinfo{precision}->[$sindex];
                my $sscale = $source_colinfo{scale}->[$sindex];

                my $dname  = $dest_colinfo{colnames}->[$dindex];
                my $dtype  = $dest_colinfo{coltypes}->[$dindex];
                my $dprec  = $dest_colinfo{precision}->[$dindex];
                my $dscale = $dest_colinfo{scale}->[$dindex];

                if ( $skipcols{ lc $sname } ) {
                    next;
                }

                my $j = 0;
                if ( $sname ne $dname ) {
                    $msg .= "Col[$sindex]: Name mismatch ($sname / $dname)\n";
                    $j++;
                }
                if ( $stype ne $dtype ) {
                    $msg .= "Col[$sindex]: Type mismatch ($stype / $dtype)\n";
                    $j++;
                }
                if ( $sprec ne $dprec ) {
                    $msg .= "Col[$sindex]: Precision mismatch ($sprec / $dprec)\n";
                    $j++;
                }
                if ( $sscale ne $dscale ) {
                    $msg .= "Col[$sindex]: Scale mismatch ($sscale / $dscale)\n";
                    $j++;
                }
                if ($j) {
                    $msg .= "\n";
                }
                $dindex++;
            }

            if ($msg) {
                $msg = "Sync-Failure: mismatched schemas\n" . "'" . $msg . "'";

                $self->_dprint($msg);

                $self->_debug
                    && print "\n\nSource(\n" . $source_dump . "\n)\n\nDest(\n" . $dest_dump . "\n)\n";

                # need to flesh out error return
                $self->{error} = $msg;
                return ( error => $self->{error}, status => "failed" );
            }
        }
    }

    #
    # Open the update/insert queries
    #
    $self->_dprint("opening insert/delete query handles for sync of $source_table to $dest_table");

    my ( $ins_cid, $del_cid, $dest_ins_qry, $dest_del_qry );
    if ( !$dry_run ) {
        my $dest_places = join( ",", ("?") x ( $#dest_cols + 1 ) );

        $dest_ins_qry = "insert into $dest_table ($dest_cols) values ($dest_places)";

        $self->_dprint("\nOpening Insert Query: $dest_ins_qry");
        $ins_cid = $dest_db->SQL_OpenBoundQuery($dest_ins_qry);

        my $dest_del_qry = "delete from $dest_table where ";
        my @where;

        foreach my $col (@dest_cols) {
            if ( $skiplong{ lc $col } ) {
                if ( ref($dest_db) =~ /Oracle/ ) {
                    push( @where, "(dbms_lob.compare($col,?)=0 or (? is null and $col is null))" );
                }
                else {
               # This is bad - it can result in deleting a row we just inserted due to ignoring the field
               # should treat this as a failure/error condition if we don't have a suitable long field comparison method
                    push( @where, "(? is null or ? is not null)" );
                }
            }
            else {
                push( @where, "($col=? or (? is null and $col is null))" );
            }
        }
        if ($dest_where) {
            push( @where, "($dest_where)" );
        }

        $dest_del_qry .= join( " and ", @where );

        # If no unique column sets, then we have to limit to a single delete
        # otherwise, we know there are no duplicates
        if ( scalar(@unique_info) == 0 && !$no_dups ) {
            if ( ref($dest_db) =~ /Oracle/ ) {
                $dest_del_qry .= " and rownum=1 ";
            }
            elsif ( ref($dest_db) =~ /MySQL/ ) {
                $dest_del_qry .= " limit 1";
            }
            else {
                $self->{error} = "unable to limit delete to single row";
                return ( error => $self->{error}, status => "failed" );
            }
        }

        $self->_dprint("\nOpening Delete Query: $dest_del_qry");
        $del_cid = $dest_db->SQL_OpenBoundQuery($dest_del_qry);

        #
        # Open the "unique'ing" delete queries
        #
        foreach my $uref (@unique_info) {
            my @where = ();

            foreach my $col (@dest_cols) {
                if ( $uref->{fields}->{ uc $col } ) {
                    if ( $skiplong{ lc $col } ) {
                        push( @where, "(? is null or ? is not null)" );
                    }
                    else {
                        push( @where, "($col=? or (? is null and $col is null))" );
                    }
                }
                else {

                    # yes, this is a hack, but it allows for must faster code later
                    # since it doesn't have to worry about building a column list
                    push( @where, "(? is null or ? is not null)" );
                }
            }

            if ($dest_where) {
                push( @where, "($dest_where)" );
            }

            $uref->{qry} = "delete from $dest_table where " . join( " and ", @where );
            $uref->{cid} = $dest_db->SQL_OpenBoundQuery( $uref->{qry} );
        }
    }

    #
    # Rus the pre_select_check callback
    #
    if ( $opts{pre_select_check} ) {
        my $res = $opts{pre_select_check}->(%opts);
        if ($res) {
            return (
                error  => "pre_select_check failed: $res",
                status => "failed"
            );
        }
    }

    #
    # Turn off autocommit if we're going to do any updates
    #
    if ( !$dry_run ) {
        $dest_db->SQL_AutoCommit(0);
    }

    #
    # Main processing loop
    #
    my $more_source = 1;
    my $more_dest   = 1;

    my $src_row  = undef;
    my $dest_row = undef;

    my $seen_source_rows = 0;
    my $seen_dest_rows   = 0;
    my $matching_rows    = 0;
    my $inserts          = 0;
    my $deletes          = 0;
    my $pending          = 0;
    my $commits          = 0;

    my $elap_fetch_source = 0;
    my $elap_fetch_dest   = 0;

    my $hit_max_inserts = 0;
    my $hit_max_deletes = 0;
    my $hit_unique      = 0;

    my $status = "ok";

    $self->_dprint("\nOpening select queries...");

    #
    # Source table
    #
    my $source_sel_qry = "select";
    if ($no_dups) {
        $source_sel_qry .= " distinct";
    }
    $source_sel_qry .= " $source_cols from $source_table $source_alias";
    if ($source_where) {
        $source_sel_qry .= " where $source_where";
    }
    $source_sel_qry .= " order by $source_sort_cols";
    $self->_dprint("\nOpening Source Select Query: $source_sel_qry");
    my $source_cid = $source_db->SQL_OpenQuery($source_sel_qry);
    if ( !$source_cid ) {
        $self->{error} = "opening source select failed: " . $source_db->SQL_ErrorString();
        if ( !$dry_run ) {
            $dest_db->SQL_RollBack();
        }
        return ( error => $self->{error}, status => "failed" );
    }

    #
    # Destination table
    #
    my $dest_sel_qry = "select";
    if ($no_dups) {
        $dest_sel_qry .= " distinct";
    }
    $dest_sel_qry .= " $dest_cols from $dest_table $dest_alias";
    if ($dest_where) {
        $dest_sel_qry .= " where $dest_where";
    }
    $dest_sel_qry .= " order by $dest_sort_cols";
    $self->_dprint("\nOpening Dest Select Query: $dest_sel_qry");
    my $dest_cid = $dest_db->SQL_OpenQuery($dest_sel_qry);
    if ( !$dest_cid ) {
        $self->{error} = "opening dest select failed: " . $dest_db->SQL_ErrorString();
        if ( !$dry_run ) {
            $dest_db->SQL_RollBack();
        }
        return ( error => $self->{error}, status => "failed" );
    }

    if ($dumpfile) {
        my $csv = Text::CSV->new( { binary => 1 } );

        $self->_dprint("\nDumping content of original destination table.");
        open( my $out, ">${dumpfile}.dest-pre.csv" );
        my $tmp_dest_cid = $dest_db->SQL_OpenQuery($dest_sel_qry);
        while ( my @tmp = $dest_db->SQL_FetchRow($tmp_dest_cid) ) {
            my $status = $csv->combine(@tmp);
            print $out $csv->string(), "\n";
        }
        $dest_db->SQL_CloseQuery($tmp_dest_cid);
        close($out);

    }

MAIN: while ( $more_source || $more_dest ) {

        #
        # Commit progress periodically if we are in force mode
        #
        if ( $force && $pending > 500 ) {
            $self->_dprint("max pending updates reached, committing.");
            if ( !$dry_run ) {
                $dest_db->SQL_Commit();
            }
            $pending = 0;
            $commits++;
        }

        #
        # Validate that we haven't exceeded any thresholds, and if we are not
        # forcing or dryrun, stop processing and roll back
        #
        if ( !$force ) {
            if (   !$hit_max_deletes
                && defined($max_deletes)
                && $deletes >= $max_deletes )
            {
                $status          = "failed";
                $hit_max_deletes = 1;          # only report error first time

                $self->_dprint("max deletes reached ($max_deletes)");
                $self->{error} = "max deletes reached";

                # If we're doing a dry run, continue through loop
                if ( !$dry_run ) {
                    $self->_dprint("rolling back updates.");
                    if ( !$dry_run ) {
                        $dest_db->SQL_RollBack();
                    }
                    last MAIN;
                }
            }

            if (   !$hit_max_inserts
                && defined($max_inserts)
                && $inserts >= $max_inserts )
            {
                $status          = "failed";
                $hit_max_inserts = 1;          # only report error first time

                $self->_dprint("max inserts reached ($max_inserts)");
                $self->{error} = "max inserts reached";

                # If we're doing a dry run, continue through loop
                if ( !$dry_run ) {
                    $self->_dprint("rolling back updates.");
                    if ( !$dry_run ) {
                        $dest_db->SQL_RollBack();
                    }
                    last MAIN;
                }
            }
        }

        #
        # Attempt to read a row from source (if needed)
        #
        if ( !$src_row && $more_source ) {
            my $fetch_st    = time;
            my $tmp_src_row = $source_db->SQL_FetchRowRef($source_cid);
            my $fetch_et    = time;
            $elap_fetch_source += ( $fetch_et - $fetch_st );
            if ( $source_db->SQL_ErrorCode() ) {
                $self->{error} = "select from source failed: " . $source_db->SQL_ErrorString();
                $status = "failed";
                last MAIN;
            }
            elsif ($tmp_src_row) {
                $seen_source_rows++;
                $src_row = $tmp_src_row;

                if ( $seen_source_rows % 1000 == 0 ) {
                    $self->_dprint("read $seen_source_rows rows from source");
                }

                $self->_dprintrowall( "FetchSrc", $src_row );
                next MAIN;
            }
            else {
                $more_source = 0;
                next MAIN;
            }
        }

        #
        # Attempt to read a row from destination (if needed)
        #
        if ( !$dest_row && $more_dest ) {
            my $fetch_st     = time;
            my $tmp_dest_row = $dest_db->SQL_FetchRowRef($dest_cid);
            my $fetch_et     = time;
            $elap_fetch_dest += ( $fetch_et - $fetch_st );
            if ( $dest_db->SQL_ErrorCode() ) {
                $self->{error} = "select from dest failed: " . $dest_db->SQL_ErrorString();
                $status = "failed";
                last MAIN;
            }
            elsif ($tmp_dest_row) {
                $seen_dest_rows++;
                $dest_row = $tmp_dest_row;

                if ( $seen_dest_rows % 1000 == 0 ) {
                    $self->_dprint("read $seen_dest_rows rows from destination");
                }

                $self->_dprintrowall( "FetchDst", $src_row );
                next MAIN;
            }
            else {
                $more_dest = 0;
                next MAIN;
            }
        }

        #
        # Safety check
        #
        if ( !$src_row && !$dest_row ) {
            last MAIN;
        }

        #
        # Do row comparisons and act process through rows
        # Need to re-order this section for efficiency later
        #
        my $rel = $self->_compare( $src_row, $dest_row, \@coltypes );

        # have a pair of matching rows in source and destination
        if ( $src_row && $dest_row && $rel == 0 ) {

            # change 0 to 1 for temporarily enabling this debug line, normally it
            # is way too much output, and not useful for routine debugging of sync
            # operations
            0
                && $self->_dprint("matching rows, skipping rows. (src=$seen_source_rows, dest=$seen_dest_rows)");
            $matching_rows++;
            undef $src_row;
            undef $dest_row;
            next MAIN;
        }

        # out of source rows/we don't have one
        elsif ( !$src_row || ( defined($rel) && $rel > 0 ) ) {
            $deletes++;
            if ( !$src_row ) {
                $self->_dprint("($deletes) out of source rows, deleting destination row.");
            }
            else {
                $self->_dprint("($deletes) source row sorts after dest row, deleting dest row.");

                $self->_dprintrow( "CurSrc", $src_row );
                $self->_dprintrow( "CurDst", $dest_row );
            }

            if ( !$dry_run ) {
                my @vals = map { $_, $_ } @{$dest_row};
                $self->_dprintrow( "deleting", $dest_row );
                my $res = $dest_db->SQL_ExecQuery( $del_cid, @vals );
                if ( !$res ) {
                    $self->{error} = "delete from dest failed: " . $dest_db->SQL_ErrorString();
                    $status = "failed";
                    last MAIN;
                }
                else {
                    $pending++;
                }
                my $cnt = $dest_db->SQL_RowCount($del_cid);
                $self->_dprintrow( "deleted ($cnt)", $dest_row );
            }

            undef $dest_row;
            next MAIN;
        }

        # out of dest rows/we don't have one
        elsif ( !$dest_row || ( defined($rel) && $rel < 0 ) ) {
            $inserts++;
            if ( !$dest_row ) {
                $self->_dprint("($inserts) out of dest rows, inserting source row.");
            }
            else {
                $self->_dprint("($inserts) source row sorts before dest row, inserting source row.");

                $self->_dprintrow( "CurSrc", $src_row );
                $self->_dprintrow( "CurDst", $dest_row );
            }

            if ( !$dry_run ) {
                my @vals = map { $_, $_ } @{$src_row};
                $self->_dprintrow( "deleting (unique)", $src_row );
                foreach my $uref (@unique_info) {
                    my $res = $dest_db->SQL_ExecQuery( $uref->{cid}, @vals );
                    if ( !$res ) {
                        $self->{error} = "delete unique from dest failed: " . $dest_db->SQL_ErrorString();
                        $status = "failed";
                        last MAIN;
                    }
                    else {
                        $pending++;
                    }
                    my $cnt = $dest_db->SQL_RowCount( $uref->{cid} );
                    $self->_dprintrow( "deleted (unique) ($cnt)", $src_row );
                }

                $self->_dprintrow( "inserting", $src_row );
                my $res = $dest_db->SQL_ExecQuery( $ins_cid, @{$src_row} );
                if ( !$res ) {
                    $self->_dprint("hit unique constraint violation");
                    $self->{error} = "insert into dest failed: " . $dest_db->SQL_ErrorString();
                    $status = "failed";
                    last MAIN;
                }
                else {
                    $pending++;
                }
            }

            undef $src_row;
            next MAIN;
        }

        # this shouldn't happen
        else {
            $self->_dprint("shouldn't get here.");
            $status = "failed";
            die "should not get here!";
        }
    }
    $self->_dprint("closing select queries...");
    $source_db->SQL_CloseQuery($source_cid);
    $dest_db->SQL_CloseQuery($dest_cid);

    #
    # Check for empty source table
    #
    if ($check_empty_source) {
        if ( ( $matching_rows + $inserts ) < 1 || ( $seen_source_rows < 1 ) ) {
            my $err
                = "Check for empty source table failed. (Matching=$matching_rows Inserts=$inserts SeenSource=$seen_source_rows)";
            if ( $self->{error} ) {
                $err .= " Previous/nested error (" . $self->{error} . ")";
            }
            $self->_dprint( "\n" . $err );
            $dest_db->SQL_RollBack();
            return (
                error  => $err,
                status => "failed"
            );
        }
    }

    if ($dumpfile) {
        $self->_dprint("\nDumping content of source table.");
        my $csv = Text::CSV->new( { binary => 1 } );

        open( my $out, ">${dumpfile}.src.csv" );
        my $src_cid = $source_db->SQL_OpenQuery($source_sel_qry);
        while ( my @tmp = $source_db->SQL_FetchRow($src_cid) ) {
            my $status = $csv->combine(@tmp);
            print $out $csv->string(), "\n";
        }
        $source_db->SQL_CloseQuery($src_cid);
        close($out);

        $self->_dprint("\nDumping content of final destination table.");
        open( my $out, ">${dumpfile}.dest.csv" );
        my $dest_cid = $dest_db->SQL_OpenQuery($dest_sel_qry);
        while ( my @tmp = $dest_db->SQL_FetchRow($dest_cid) ) {
            my $status = $csv->combine(@tmp);
            print $out $csv->string(), "\n";
        }
        $dest_db->SQL_CloseQuery($dest_cid);
        close($out);

    }

    $self->_dprint("getting final row count");
    my $final_row_count;
    my $final_cnt_qry = "select count(*) from $dest_table $dest_alias";
    $self->_dprint("\nOpening Final Count Query: $final_cnt_qry");
    my $final_cnt_cid = $dest_db->SQL_OpenQuery($final_cnt_qry);
    if ( !$final_cnt_cid ) {
        my $err = "opening dest count select failed: " . $dest_db->SQL_ErrorString();
        $dest_db->SQL_RollBack();
        return (
            error  => $err,
            status => "failed"
        );
    }
    else {
        ($final_row_count) = $dest_db->SQL_FetchRow($final_cnt_cid);
        $dest_db->SQL_CloseQuery($final_cnt_cid);

        if ( !$ignore_row_count && $final_row_count != $seen_source_rows ) {
            $dest_db->SQL_RollBack();
            return (
                error =>
                    "final dest row count ($final_row_count) did not match source ($seen_source_rows), check primary key definition",
                status => "failed"
            );
        }
    }
    $self->_dprint("final row count = $final_row_count");

    #
    # Run the post_sync_check callback
    #
    if ( $opts{post_sync_check} ) {
        $self->_dprint("\nRunning post sync check function.");
        my $res = $opts{post_sync_check}->(%opts);
        if ($res) {
            $dest_db->SQL_RollBack();
            return (
                error  => "post_sync_check failed: $res",
                status => "failed"
            );
        }
    }

    $self->_dprint("closing queries...");
    if ( !$dry_run ) {
        if ( !$self->{error} ) {
            if ($pending) {
                $self->_dprint("pending changes, issuing commit.");
                $dest_db->SQL_Commit();
                $commits++;
            }
        }
        $dest_db->SQL_CloseQuery($del_cid);
        $dest_db->SQL_CloseQuery($ins_cid);

        foreach my $uref (@unique_info) {
            $dest_db->SQL_CloseQuery( $uref->{cid} );
        }

        $dest_db->SQL_AutoCommit(1);
    }

    $self->_dprint("done with sync of $source_table to $dest_table");

    #
    # Run the post_check callback
    #
    if ( $opts{post_commit_check} ) {
        my $res = $opts{post_commit_check}->(%opts);
        if ($res) {
            return (
                error  => "post_commit_check failed: $res",
                status => "failed"
            );
        }
    }

    $self->{end_time}       = time;
    @tmp_times              = times;
    $self->{end_user_cpu}   = $tmp_times[0];
    $self->{end_system_cpu} = $tmp_times[1];

    return (
        status               => $status,
        error                => $self->{error},
        inserts              => $inserts,
        deletes              => $deletes,
        seen_source_rows     => $seen_source_rows,
        seen_dest_rows       => $seen_dest_rows,
        final_dest_rows      => $final_row_count,
        matching_rows        => $matching_rows,
        commits              => $commits,
        elapsed              => $self->{end_time} - $self->{start_time},
        elapsed_user_cpu     => $self->{end_user_cpu} - $self->{start_user_cpu},
        elapsed_system_cpu   => $self->{end_system_cpu} - $self->{start_system_cpu},
        elapsed_fetch_source => $elap_fetch_source,
        elapsed_fetch_dest   => $elap_fetch_dest,
    );
}

# Begin-Doc
# Name: _compar
# Type: method
# Description: Performs a columnwise comparison of two rows, returning -1,0,1 similar to cmp
# Syntax: $obj->_compare($srow,$drow,$coltypesref)
# Comments: -1: srow < drow, 0: equal, 1: srow > drow
# Comments: returns undef if either row not defined
# Comments: coltypesref is a ref to an array of "string" or "numeric", directing which type of comparison
# will be used for each column
# End-Doc
sub _compare {
    my $self        = shift;
    my $srow        = shift;
    my $drow        = shift;
    my $coltypesref = shift;
    my @coltypes    = @{$coltypesref};

    if ( !$srow ) { return undef; }
    if ( !$drow ) { return undef; }

    my $cols = scalar( @{$srow} );

    #
    # Would be better to build this entire routine as an eval
    #
    for ( my $i = 0; $i <= $cols; $i++ ) {
        my $tmp;
        if ( $coltypes[$i] eq "numeric" ) {
            $tmp = $srow->[$i] <=> $drow->[$i];
        }
        else {
            # SQL sorts nulls last
            # For oracle could use NULLS LAST clause in order by, but not MySQL

            my $a = $srow->[$i];
            my $b = $drow->[$i];
            if ( $a eq $b ) {
                $tmp = 0;
            } elsif ( $a =~ /^\s*$/ && $b !~ /^\s*$/ ) {
                $tmp = 1;
            } elsif ( $a !~ /^\s*$/ && $b =~ /^\s*$/ ) {
                $tmp = -1;
            } else {
                $tmp = $a cmp $b;
            }
        }

        if ( $tmp == 0 ) {
            next;
        }
        else {
            return $tmp;
        }
    }

    return 0;
}

# Begin-Doc
# Name: _dump_row
# Type: method
# Description: Prints out row contents for diagnostic purposes
# Syntax: $obj->_dump_row($label, $rowarrayref);
# End-Doc

sub _dump_row {
    my $self  = shift;
    my $label = shift;
    my $row   = shift;

    print $label, ":\n";
    foreach my $elem ( @{$row} ) {
        print "\t", substr( $elem, 0, 60 );
        if ( length($elem) > 60 ) {
            print "...";
        }
        print "\n";
    }
    print "\n";
}

# Begin-Doc
# Name: GetUniqueKeys
# Type: method
# Description: Returns ref to array of array refs containing the unique key sets for a given table
# Syntax: $keys = $obj->GetUniqueKeys($db, $schema, $table);
# End-Doc

sub GetUniqueKeys {
    my $self  = shift;
    my $db    = shift;
    my $owner = shift;
    my $table = shift;

    my $cache = $self->{_unique_key_cache};
    my $qry;
    my $cid;

    if ( !$cache->{$db}->{$owner}->{$table} ) {
        $qry
            = "select a.index_name,a.column_name from dba_ind_columns a, dba_indexes b "
            . "where a.index_owner=b.owner and a.index_name=b.index_name and b.uniqueness='UNIQUE' and "
            . "lower(a.table_owner)=? and lower(a.table_name)=? order by index_name,column_position";
        $cid = $db->SQL_OpenQuery( $qry, lc $owner, lc $table )
            || $db->SQL_Error($qry) && die;
        my %ukeys;
        while ( my ( $iname, $col ) = $db->SQL_FetchRow($cid) ) {
            $self->{debug} && print "Unique Index ($iname) on Col ($col)\n";
            push( @{ $ukeys{ "IDX-" . $iname } }, $col );
        }
        $db->SQL_CloseQuery($cid);

        $qry
            = "select a.constraint_name,a.column_name from dba_cons_columns a, dba_constraints b "
            . "where a.owner=b.owner and a.constraint_name=b.constraint_name and b.constraint_type='P' and "
            . "lower(b.owner)=? and lower(b.table_name)=? order by a.constraint_name,a.position";
        $cid = $db->SQL_OpenQuery( $qry, lc $owner, lc $table )
            || $db->SQL_Error($qry) && die;
        while ( my ( $cname, $col ) = $db->SQL_FetchRow($cid) ) {
            $self->{debug}
                && print "Unique Constraint ($cname) on Col ($col)\n";
            push( @{ $ukeys{ "CONS-" . $cname } }, $col );
        }
        $db->SQL_CloseQuery($cid);

        my @unique = ();
        my %seen   = ();
        foreach my $uref ( values %ukeys ) {
            my $cols = join( ",", @{$uref} );
            next if ( $seen{$cols} );
            $seen{$cols} = 1;
            push( @unique, $uref );
        }

        $cache->{$db}->{$owner}->{$table} = [@unique];
    }

    my $json = new JSON;
    $json->canonical(1);

    $self->{debug} && print "Unique Key Info: " . $json->encode($cache) . "\n";

    return $cache->{$db}->{$owner}->{$table};
}

# Begin-Doc
# Name: dump_colinfo
# Type: method
# Description: Returns string that is a textual dump of the column information
# Syntax: $str = $obj->dump_colinfo($col_info_hash_ref)
# End-Doc

sub dump_colinfo {
    my $self    = shift;
    my $colinfo = shift;
    my $res;

    $res .= "Column Count(" . $colinfo->{numcols} . ")\n";

    for ( my $i = 0; $i < $colinfo->{numcols}; $i++ ) {
        my $name  = $colinfo->{colnames}->[$i];
        my $type  = $colinfo->{coltypes}->[$i];
        my $prec  = $colinfo->{precision}->[$i];
        my $scale = $colinfo->{scale}->[$i];
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

1;
