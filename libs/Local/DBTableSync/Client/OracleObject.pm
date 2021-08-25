# Begin-Doc
# Name: Local::DBTableSync::Client::OracleObject
# Type: module
# Description: Oracle specific implementation of the DBTableSync client module
# End-Doc
package Local::DBTableSync::Client::OracleObject;
use parent "Local::DBTableSync::Client";

# Begin-Doc
# Name: init
# Type: method
# Description: kicks off initialization process for client module
# Returns: 1 on success, undef on error
# End-Doc
sub init {
    my $self     = shift;
    my $date_qry = "alter session set NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'";
    my $ts_qry   = "alter session set NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF'";

    #
    # Set appropriate default date formats
    #
    unless ( $self->{read_db}->SQL_ExecQuery($date_qry) ) {
        $self->{error}
            = ref($self) . "::init - setting of nls date format failed: " . $self->{read_db}->SQL_ErrorString();
        return undef;
    }

    unless ( $self->{read_db}->SQL_ExecQuery($ts_qry) ) {
        $self->{error}
            = ref($self) . "::init - setting of nls timestamp format failed: " . $self->{read_db}->SQL_ErrorString();
        return undef;
    }

    $self->{read_db}->dbhandle->{ChopBlanks}  = 0;
    $self->{read_db}->dbhandle->{ora_ph_type} = 96;

    if ( $self->{sep_db} && $self->{type} ne "source" ) {
        unless ( $self->{write_db}->SQL_ExecQuery($date_qry) ) {
            $self->{error}
                = ref($self) . "::init - setting of nls date format failed: " . $self->{write_db}->SQL_ErrorString();
            return undef;
        }

        unless ( $self->{write_db}->SQL_ExecQuery($ts_qry) ) {
            $self->{error}
                = ref($self)
                . "::init - setting of nls timestamp format failed: "
                . $self->{write_db}->SQL_ErrorString();
            return undef;
        }

        $self->{write_db}->dbhandle->{ChopBlanks}  = 0;
        $self->{write_db}->dbhandle->{ora_ph_type} = 96;
    }

    return $self->SUPER::init();
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
    my $cid = $db->SQL_OpenQueryExtra( $qry, { ora_pers_lob => 1 }, @{$args} );

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
            || $tname =~ /BIN/ )
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

            if ( $self->{skiplong}->{$field} ) {
                push( @where, "(dbms_lob.compare(${field},?)=0 or (? is null and ${field} is null))" );
            }
            else {
                push( @where, "(${field}=? or (? is null and ${field} is null))" );
            }
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
        if ( $self->{skiplong}->{$field} ) {
            push( @where, "(dbms_lob.compare(${field},?)=0 or (? is null and ${field} is null))" );
        }
        else {
            push( @where, "(${field}=? or (? is null and ${field} is null))" );
        }

        push( @fields, $field, $field );
    }

    $qry .= join( " and ", @where );
    if ( $self->{no_dups} ) {
        $qry .= " and rownum=1";
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
