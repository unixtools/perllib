# Begin-Doc
# Name: Local::DBTableSync::Client::PostgreSQLObject
# Type: module
# Description: PostgreSQL specific implementation of DBTableSync client module
# End-Doc
package Local::DBTableSync::Client::PostgreSQLObject;
use parent "Local::DBTableSync::Client";
use Encode;

# Begin-Doc
# Name: fetch_row
# Type: method
# Description: fetches next row in select statement
# Returns: returns arrayref of row data, undef if no more row data, and undef on error
# Comments: Implementing this as a workaround for some weird double decode utf8 behavior in DBI::Pg
#    changes are welcome to set the driver behavior properly 
# End-Doc
sub fetch_row {
    my $self = shift;
    my $db   = shift;
    my $cid  = shift;
    my $row  = $self->SUPER::fetch_row( $db, $cid );
    return undef unless $row;

    my @encoded = ();
    foreach my $val (@{ $row }) {
        push(@encoded, encode('utf-8', $val));
    }
    return \@encoded;
}

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
            || $tname =~ /TEXT/ )
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
            push( @where, "(${field}=? or (? is null and ${field} is null))" );
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
        push( @where, "(${field}=? or (? is null and ${field} is null))" );
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
