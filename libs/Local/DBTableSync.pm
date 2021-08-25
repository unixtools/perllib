#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
# This module contains portions copyright Curators of the University of Missouri.
#

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
    row_count_interval => 1000,

    # Example table: position, department, user
    # Unique index on position,department and a unique index on user (one job per userid and per position nbr)
    # if we had a composite key field, include it here as well
    # if no unique constraints exist, this attribute can be left out
    unique_keys => [
        ["position","department"],
        ["user"],
    ],
);

Note that module has only been actively tested with OracleObject and MySQLObject at this time.
 
End-Doc

=cut

package Local::DBTableSync;
require Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use JSON;
use Local::UsageLogger;
use Local::DBTableSync::Client;
use Time::HiRes qw(time);

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
#    source_args => optional query arguments (array reference) for source table query
#    dest_where => optional string, appended as where clause on select of rows from destination table
#    source_alias => alias for primary source table, can be useful in where clause
#    dest_alias => alias for primary dest table, can be useful in where clause
#
#    ukey_sort => allow sorting by a unique key instead of calculated column list.
#      Optional: pass array reference of column names to use for unique key sorting
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

    my $ignore_row_count;
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

    my $debug = $self->{debug};
    if ( exists( $opts{debug} ) ) {
        $debug = $opts{debug};
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

    if ( exists( $opts{ignore_row_count} ) ) {
        $ignore_row_count = $opts{ignore_row_count};
    }

    my $dumpfile;
    if ( exists( $opts{dumpfile} ) ) {
        $dumpfile = $opts{dumpfile};
    }

    my $row_count_interval = 1000;
    if ( exists( $opts{row_count_interval} ) ) {
        $row_count_interval = $opts{row_count_interval};
    }

    foreach my $required (qw/source_db source_table dest_db dest_table/) {
        if ( !$opts{$required} ) {
            return ( error => "missing ${required}", status => "failed" );
        }
    }

    my ( $source_table, $dest_table ) = @opts{qw/source_table dest_table/};

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
    # Allocate source/destination client handlers
    #
    my %sopts = map { my $key = $_; $key =~ s/source_//r => $opts{$_} } keys %opts;
    my %dopts = map { my $key = $_; $key =~ s/dest_//r   => $opts{$_} } keys %opts;

    #
    # Determine if we can use the Unique key based sort
    #
    my $ukey_sort = 0;

    # Override with out specific selection
    if ( exists( $opts{ukey_sort} ) ) {
        $ukey_sort = $opts{ukey_sort};
    }

    my $sclient;
    my $dclient;

    {

        # source client first
        my $submodule;
        if ( ref( $sopts{db} ) =~ m/::([a-zA-Z_]+)$/ ) {
            $submodule = $1;
        }
        my $module = "Local::DBTableSync::Client::${submodule}";
        if ( $module->can("new") ) {
            $sclient = $module->new( %sopts, debug => $debug, type => "source", ukey_sort => $ukey_sort );
        }
        else {
            return (
                error  => "unable to allocate source client: $module",
                status => "failed"
            );
        }
    }
    {

        # destination client next
        my $submodule;
        if ( ref( $dopts{db} ) eq "HASH" && ref( $dopts{db}{read} ) =~ m/::([a-zA-Z0-9_]+)$/ ) {
            $submodule = $1;
        }
        elsif ( ref( $dopts{db} ) =~ m/::([a-zA-Z0-9_]+)$/ ) {
            $submodule = $1;
        }

        my $module = "Local::DBTableSync::Client::${submodule}";
        if ( $module->can("new") ) {
            $dclient = $module->new( %dopts, debug => $debug, type => "dest", ukey_sort => $ukey_sort );
        }
        else {
            return (
                error  => "unable to allocate destination client: $module",
                status => "failed"
            );
        }
    }

    #
    # Signal client handlers to initialize
    # and handle any errors that come up
    #
    unless ( $sclient->init() ) {
        return (
            error  => "unable to initialize source client: " . $sclient->error(),
            status => "failed"
        );
    }

    unless ( $dclient->init() ) {
        return (
            error  => "unable to initialize destination client: " . $dclient->error(),
            status => "failed"
        );
    }

    my @source_cols      = @{ $sclient->colnames() };
    my @dest_cols        = @{ $dclient->colnames() };
    my $source_cols      = join( ", ", @source_cols );
    my $dest_cols        = join( ", ", @dest_cols );
    my %have_source_cols = map { $_ => 1 } @source_cols;
    my %have_dest_cols   = map { $_ => 1 } @dest_cols;
    my $col_compare      = "";

    foreach my $col (@source_cols) {
        if ( !$have_dest_cols{$col} ) {
            $col_compare .= "Column ${col} in source but not in destination.\n";
        }
    }

    foreach my $col (@dest_cols) {
        if ( !$have_source_cols{$col} ) {
            $col_compare .= "Column ${col} in destination but not in source.\n";
        }
    }

    if ( $#source_cols != $#dest_cols ) {
        my $s_cnt = $#source_cols + 1;
        my $d_cnt = $#dest_cols + 1;

        my $msg = "Sync-Failure: mismatched column counts\n";
        $msg .= "Source has ${s_cnt} columns, destination has ${d_cnt} columns.\n\n";

        if ($col_compare) {
            $msg .= $col_compare . "\n\n";
        }

        $msg .= "  Source Cols: ${source_cols}\n";
        $msg .= "  Dest Cols: ${dest_cols}\n";

        $self->_dprint($msg);
        $self->{error} = $msg;
        return ( error => $self->{error}, status => "failed" );
    }

    #
    # Compare the schemas to make certain that they are identical, but only
    # if the compare_schema option is enabled.
    #
    if ($compare_schemas) {
        my $source_dump = $sclient->dump_colinfo();
        my $dest_dump   = $dclient->dump_colinfo();

        # Short circuit check
        if ( $source_dump ne $dest_dump ) {
            my $msg = "";

            if ($col_compare) {
                $msg .= $col_compare . "\n";
            }

            my %source_colinfo = %{ $sclient->colinfo() };
            my %dest_colinfo   = %{ $dclient->colinfo() };
            my %skipcols       = %{ $sclient->skipcols() };

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
                    $msg .= "Col[$sindex] ($sname): Name mismatch ($sname / $dname)\n";
                    $j++;
                }
                if ( $stype ne $dtype ) {
                    $msg .= "Col[$sindex] ($sname): Type mismatch ($stype / $dtype)\n";
                    $j++;
                }
                if ( $sprec ne $dprec ) {
                    $msg .= "Col[$sindex] ($sname): Precision mismatch ($sprec / $dprec)\n";
                    $j++;
                }
                if ( $sscale ne $dscale ) {
                    $msg .= "Col[$sindex] ($sname): Scale mismatch ($sscale / $dscale)\n";
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
    # Runs the pre_select_check callback
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
    # Main processing loop
    #
    my $more_source       = 1;
    my $more_dest         = 1;
    my $src_row           = undef;
    my $dest_row          = undef;
    my $seen_source_rows  = 0;
    my $seen_dest_rows    = 0;
    my $matching_rows     = 0;
    my $inserts           = 0;
    my $deletes           = 0;
    my $elap_fetch_source = 0;
    my $elap_fetch_dest   = 0;
    my $hit_max_inserts   = 0;
    my $hit_max_deletes   = 0;
    my $hit_unique        = 0;
    my $status            = "ok";

    if ($dumpfile) {
        $self->_dprint("\nDumping content of original destination table.");

        if ( !$dclient->dump_table( "${dumpfile}.dest-pre.csv", "read_db" ) ) {
            return ( error => $dclient->error(), status => "failed" );
        }
    }

MAIN: while ( $more_source || $more_dest ) {

        #
        # Commit progress periodically if we are in force mode
        #
        if ( !$dclient->check_pending() ) {
            $self->{error} = $dclient->error();
            $status = "failed";
            last MAIN;
        }

        #
        # Attempt to read a row from source (if needed)
        #
        if ( !$src_row && $more_source ) {
            my $fetch_st    = time;
            my $tmp_src_row = $sclient->fetch_row();
            my $fetch_et    = time;
            $elap_fetch_source += ( $fetch_et - $fetch_st );
            if ( !$tmp_src_row && $sclient->error() ) {
                $self->{error} = "select from source failed: " . $sclient->error();
                $status = "failed";
                last MAIN;
            }
            elsif ($tmp_src_row) {
                $seen_source_rows++;
                $src_row = $tmp_src_row;

                if ( $seen_source_rows % $row_count_interval == 0 ) {
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
            my $tmp_dest_row = $dclient->fetch_row();
            my $fetch_et     = time;
            $elap_fetch_dest += ( $fetch_et - $fetch_st );
            if ( !$tmp_dest_row && $dclient->error() ) {
                $self->{error} = "select from dest failed: " . $dclient->error();
                $status = "failed";
                last MAIN;
            }
            elsif ($tmp_dest_row) {
                $seen_dest_rows++;
                $dest_row = $tmp_dest_row;

                if ( $seen_dest_rows % $row_count_interval == 0 ) {
                    $self->_dprint("read $seen_dest_rows rows from destination");
                }

                $self->_dprintrowall( "FetchDst", $dest_row );
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
        my $rel = $self->_compare( $src_row, $dest_row, $sclient->coltypes() );

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
                $self->_dprintrow( "deleting", $dest_row );
                my $cnt = $dclient->delete_row( @{$dest_row} );
                if ( !defined($cnt) ) {
                    $self->{error} = "delete from dest failed: " . $dclient->error();
                    $status = "failed";
                    last MAIN;
                }
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
                $self->_dprintrow( "deleting (unique)", $src_row );
                my $cnt = $dclient->delete_uniq( @{$src_row} );

                if ( !defined($cnt) ) {
                    $self->{error} = "delete unique from dest failed: " . $dclient->error();
                    $status = "failed";
                    last MAIN;
                }

                $self->_dprintrow( "deleted (unique) ($cnt)", $src_row );
                $self->_dprintrow( "inserting",               $src_row );
                if ( !$dclient->insert_row( @{$src_row} ) ) {
                    $self->_dprint( "insert into dest failed (" . $dclient->error() . ")" );
                    $self->{error} = "insert into dest failed: " . $dclient->error();
                    $status = "failed";
                    last MAIN;
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

    #
    # Check for any errors from the Main processing loop
    #
    if ( $self->{error} ) {
        $self->_dprint( "\nEncountered error from Main processing loop: " . $self->{error} );
        $dclient->roll_back();
        return (
            error  => $self->{error},
            status => $status
        );
    }

    #
    # Check for empty source table
    #
    if ($check_empty_source) {
        if ( ( $matching_rows + $inserts ) < 1 || ( $seen_source_rows < 1 ) ) {
            my $err
                = "Check for empty source table failed. (Matching=$matching_rows Inserts=$inserts SeenSource=$seen_source_rows)";

            $self->_dprint( "\n" . $err );
            $dclient->roll_back();
            return (
                error  => $err,
                status => "failed"
            );
        }
    }

    if ($dumpfile) {
        $self->_dprint("\nDumping content of source table.");
        if ( !$sclient->dump_table("${dumpfile}.src.csv") ) {
            return ( error => $sclient->error(), status => "failed" );
        }

        $self->_dprint("\nDumping content of final destination table.");
        if ( !$dclient->dump_table("${dumpfile}.dest.csv") ) {
            return ( error => $dclient->error(), status => "failed" );
        }
    }

    my $final_row_count;
    if ( !$self->{error} ) {
        $self->_dprint("getting final row count");
        $final_row_count = $dclient->row_count();

        if ( !defined($final_row_count) ) {
            return ( error => $dclient->error(), status => "failed" );
        }

        if ( !$ignore_row_count && $final_row_count != $seen_source_rows ) {
            return (
                error =>
                    "final dest row count (${final_row_count}) did not match source (${seen_source_rows}), check primary key definition",
                status => "failed"
            );
        }
        $self->_dprint("final row count = $final_row_count");
    }

    #
    # Run the post_sync_check callback
    #
    if ( $opts{post_sync_check} && !$self->{error} ) {
        $self->_dprint("\nRunning post sync check function.");
        my $res = $opts{post_sync_check}->(%opts);
        if ($res) {
            $dclient->roll_back();
            return (
                error  => "post_sync_check failed: $res",
                status => "failed"
            );
        }
    }

    $self->_dprint("closing queries...");

    $sclient->close_queries();
    if ( !$dclient->close_queries() ) {
        return (
            error  => $dclient->error(),
            status => "failed",
        );
    }

    $self->_dprint("done with sync of $source_table to $dest_table");

    #
    # Run the post_check callback
    #
    if ( $opts{post_commit_check} && !$self->{error} ) {
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
        inserts              => $dclient->inserts(),
        deletes              => $dclient->deletes(),
        commits              => $dclient->commits(),
        seen_source_rows     => $seen_source_rows,
        seen_dest_rows       => $seen_dest_rows,
        final_dest_rows      => $final_row_count,
        matching_rows        => $matching_rows,
        elapsed              => $self->{end_time} - $self->{start_time},
        elapsed_user_cpu     => $self->{end_user_cpu} - $self->{start_user_cpu},
        elapsed_system_cpu   => $self->{end_system_cpu} - $self->{start_system_cpu},
        elapsed_fetch_source => $elap_fetch_source,
        elapsed_fetch_dest   => $elap_fetch_dest,
    );
}

# Begin-Doc
# Name: _compare
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

            # apply "nulls last" logic to numeric comparator
            my $a = $srow->[$i];
            my $b = $drow->[$i];

            if ( !defined $a && defined $b ) {
                $tmp = 1;
            }
            elsif ( defined $a && !defined $b ) {
                $tmp = -1;
            }
            else {
                $tmp = $srow->[$i] <=> $drow->[$i];
            }
        }
        else {
            # SQL sorts nulls last
            # For oracle could use NULLS FIRST clause in order by to match cmp behavior, but not MySQL
            # May need to have this be aware of distinction between empty string and null

            my $a = $srow->[$i] . "";
            my $b = $drow->[$i] . "";
            if ( $a eq $b ) {
                $tmp = 0;
            }
            elsif ( $a eq "" && $b ne "" ) {
                $tmp = 1;
            }
            elsif ( $a ne "" && $b eq "" ) {
                $tmp = -1;
            }
            else {
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
        my %ukeys;
        my $db_ref = ref($db);
        if ( $db_ref =~ /Oracle/ ) {
            $qry
                = "select a.index_name,a.column_name from dba_ind_columns a, dba_indexes b "
                . "where a.index_owner=b.owner and a.index_name=b.index_name and b.uniqueness='UNIQUE' and "
                . "lower(a.table_owner)=? and lower(a.table_name)=? order by index_name,column_position";
            $cid = $db->SQL_OpenQuery( $qry, lc $owner, lc $table )
                || $db->SQL_Error($qry) && die;
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
        }
        elsif ( $db_ref =~ /MySQL/ ) {
            $qry
                = "select constraint_name, column_name from information_schema.key_column_usage "
                . "where lower(table_schema) = ? and lower(table_name) = ? "
                . "order by constraint_name, ordinal_position";
            $cid = $db->SQL_OpenQuery( $qry, lc $owner, lc $table ) || $db->SQL_Error($qry) && die;
            while ( my ( $cname, $col ) = $db->SQL_FetchRow($cid) ) {
                $self->{debug} && print "Constraint ($cname) on Col ($col)\n";
                push( @{ $ukeys{$cname} }, $col );
            }
            $db->SQL_CloseQuery($cid);
        }
        elsif ( $db_ref =~ /PostgreSQL/ ) {
            $qry = q{
                    select t.constraint_name,
                           k.column_name
                      from information_schema.table_constraints t
                inner join information_schema.key_column_usage k
                        on t.constraint_name = k.constraint_name
                       and t.table_schema = k.table_schema
                       and t.table_name = k.table_name 
                     where lower(t.table_schema) = ?
                       and lower(t.table_name) = ?
                       and t.constraint_type = 'PRIMARY KEY'
                  order by k.ordinal_position asc
            };
            $cid = $db->SQL_OpenQuery( $qry, lc $owner, lc $table ) || $db->SQL_Error($qry) && die;
            while ( my ( $cname, $col ) = $db->SQL_FetchRow($cid) ) {
                $self->{debug} && print "Constraint ($cname) on Col ($col)\n";
                push( @{ $ukeys{$cname} }, $col );
            }
            $db->SQL_CloseQuery($cid);
        }
        else {
            die "Unsupported DBObject ${db_ref}\n";
        }

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
        my $name  = uc $colinfo->{colnames}->[$i];
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
