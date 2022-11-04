# Begin-Doc
# Name: Local::DBTableSync::Client::MySQLObject
# Type: module
# Description: MySQL-specific implementation of the DBTableSync client module
# End-Doc
package Local::DBTableSync::Client::MySQLObject;
use parent "Local::DBTableSync::Client";

# Begin-Doc
# Name: _build_coltypes
# Type: method
# Description: analyzes the schema of the table/query in question to determine each column type
#              this information is used later in the comparison routine in Local::DBTableSync
# End-Doc
sub _build_coltypes {
    my $self = shift;

    $self->{skipcols} = {};
    $self->{skiplong} = {};
    $self->{coltypes} = undef;
    $self->{types}    = {};

    my $dbh = $self->{read_db}->dbhandle;
    my $tia = $dbh->type_info_all;

    my %sql_type_to_name = ();
    foreach my $entry ( @{$tia} ) {
        if ( ref($entry) eq "ARRAY" ) {
            my ( $name, $itype ) = @{$entry};

            next if ( $sql_type_to_name{$itype} );
            $sql_type_to_name{$itype} = $name;
        }
    }

    my @names = map { lc $_ } @{ $self->{colinfo}->{colnames} };
    my @types = @{ $self->{colinfo}->{coltypes} };
    my %types;

    @types{@names} = @types;

    foreach my $name (@names) {
        my $type  = $types{$name};
        my $tname = uc $sql_type_to_name{$type};

        # Check for excluded columns
        if ( exists( $self->{excl_cols}->{$name} ) ) {
            $self->{skipcols}->{$name} = 1;
            next;
        }

        if ( exists( $self->{mask_cols}->{$name} ) ) {
            $self->{types}->{$name} = "string";
        }
        elsif ($tname =~ /CHAR/
            || $tname =~ /TIME/
            || $tname =~ /DATE/
            || $tname =~ /BIN/
            || $tname =~ /BLOB/ )
        {
            $self->{types}->{$name} = "string";
        }
        elsif ( $tname =~ /RAW/ ) {

            # can't handle LONG RAW right now
            $self->{types}->{$name}    = "unknown";
            $self->{skipcols}->{$name} = 1;
        }
        elsif ( $tname =~ /LONG/ || $type == 40 ) {

            # 40 = CLOB
            $self->{types}->{$name}    = "string";
            $self->{skiplong}->{$name} = 1;
        }
        elsif ( $tname =~ /BFILE/ ) {
            $self->{types}->{$name}    = "unknown";
            $self->{skipcols}->{$name} = 1;
        }
        elsif ($tname =~ /DEC/
            || $tname =~ /INT/
            || $tname =~ /NUM/
            || $tname =~ /DOUBLE/
            || $tname =~ /FLOAT/ )

        {
            $self->{types}->{$name} = "numeric";
        }
        else {
            $self->{error} = ref($self) . "::_build_coltypes - don't know how to compare $name (type $type [$tname])";
            return undef;
        }
    }

    return 1;
}

# Begin-Doc
# Name: _build_collists
# Type: method
# Description: builds the following column lists
#       $self->{select_cols} - arrayref (ordered) list of database-specific column statements - used to build select statements
#       $self->{colnames}    - arrayref of lowercase column names
#       $self->{sort_cols}   - arrayref order in which results of the select statements will be ordered - ensure NULLs first
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

                push( @{ $self->{sort_cols} }, "`${col}` IS NULL" );
                push( @{ $self->{sort_cols} }, "`${col}`" );
            }

            @lower_cols = sort { $colranks{$a} <=> $colranks{$b} } @lower_cols;
        }
    }

    foreach my $col (@lower_cols) {
        unless ( $self->{skipcols}->{$col} ) {
            if ( exists( $self->{mask_cols}->{$col} ) && $self->{type} eq "source" ) {
                my $tcol = $self->{read_db}->SQL_QuoteString( $self->{mask_cols}->{$col} ) . " as " . $col;
                push( @{ $self->{select_cols} }, $tcol );
            }
            else {
                push( @{ $self->{select_cols} }, "`${col}`" );
            }
            push( @{ $self->{colnames} },    $col );
            push( @{ $self->{insert_cols} }, "`${col}`" );
        }

        unless ( $self->{skipcols}->{$col} || $self->{skiplong}->{$col} ) {
            if ($default_ordering) {

                # Force MySQL to NULLS first ordering
                push( @{ $self->{sort_cols} }, "`${col}` IS NULL" );
                push( @{ $self->{sort_cols} }, "`${col}`" );
            }
        }
    }

    return 1;
}

# Begin-Doc
# Name: _build_delete
# Type: method
# Description: builds internal delete queries for later use
# End-Doc
sub _build_delete {
    my $self = shift;

    # check/validate unique_keys
    # if valid, build unique deletes
    my $table      = $self->{table};
    my %valid_cols = map { $_ => 1 } @{ $self->colnames() };
    foreach my $keys ( @{ $self->{unique_keys} } ) {
        next unless scalar @{$keys};

        my @fields = ();
        my @where  = ();
        my $qry    = "delete from ${table} where ";

        foreach my $field ( map { lc $_ } @{$keys} ) {
            if ( !$valid_cols{$field} ) {
                $self->{error} = ref($self) . "::_build_delete - invalid column name for key (${field})";
                return undef;
            }
            push( @where, "(`${field}`=? or (? is null and `${field}` is null))" );
            push( @fields, $field, $field );
        }

        $qry .= join( " and ", @where );

        $self->_dprint("\nOpening (unique) delete query: ${qry}");
        my $cid = $self->{write_db}->SQL_OpenBoundQuery($qry);
        unless ($cid) {
            $self->{error}
                = ref($self)
                . "::_build_delete - unable to open unique delete query - "
                . $self->{write_db}->SQL_ErrorString();
            return undef;
        }

        my $qryref = {
            qry    => $qry,
            cid    => $cid,
            fields => [@fields],
            db     => $self->{write_db},
        };

        push( @{ $self->{queries}->{delete_uniq} }, $qryref );
    }

    # build generic delete
    my @fields = ();
    my @where  = ();
    my $qry    = "delete from ${table} where ";

    foreach my $field ( @{ $self->colnames() } ) {
        push( @where, "(`${field}`=? or (? is null and `${field}` is null))" );
        push( @fields, $field, $field );
    }

    $qry .= join( " and ", @where );
    if ( $self->{no_dups} ) {
        $qry .= " limit 1";
    }

    $self->_dprint("\nOpening (specific) delete query: ${qry}");

    my $cid = $self->{write_db}->SQL_OpenBoundQuery($qry);
    unless ($cid) {
        $self->{error}
            = ref($self)
            . "::_build_delete - unable to open generic delete query - "
            . $self->{write_db}->SQL_ErrorString();
        return undef;
    }

    my $qryref = {
        qry    => $qry,
        cid    => $cid,
        fields => [@fields],
        db     => $self->{write_db},
    };

    $self->{queries}->{delete} = $qryref;
    return 1;
}

1;
