#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
#

=begin
Begin-Doc
Name: Local::DBQueue
Type: module
Description: simple queue management object with *DBObject backend. Right now only works with MySQLObject.

Comments: 

Required Schema:

drop table if exists work_queue;
create table work_queue
(
    queue varchar(100) not null,
    itemid varchar(100) not null,
    meta varchar(2000),
    grabbed varchar(1) default 'N' not null,
    queuetime datetime not null,
    grabtime datetime,
    grabhost varchar(100),
    grabpid integer,
    attempts integer default 0
) engine=MyISAM;
create unique index wq_id on work_queue(itemid,queue);
create index wq_qt on work_queue(queuetime,queue);
create index wq_ghgpq on work_queue(grabhost,grabpid,queue);

Does not have to be MyISAM, but performance seems to be better
with large queues and fewer odd InnoDB deadlock issues.

End-Doc

=cut

package Local::DBQueue;
use Exporter;
use strict;
use Carp;
use Sys::Hostname;
use JSON;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use Local::UsageLogger;

@ISA    = qw(Exporter);
@EXPORT = qw();

BEGIN {
    &LogAPIUsage();
}

=begin
Begin-Doc
Name: new
Type: method
Description: creates a new db queue object
Syntax: $obj = new Local::DBQueue(db => $dbobject, table => "work_queue")
Comments: Must be passed a database object handle
End-Doc
=cut

sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;
    my $tmp   = {};

    $tmp->{"db"} = $opts{db} || croak "Must specify database.";

    if ( ref( $tmp->{db} ) !~ /MySQL/ ) {
        croak "Requires MySQLObject at this time.";
    }
    $tmp->{"table"} = $opts{table} || "work_queue";

    &LogAPIUsage();

    bless $tmp, $class;

    return $tmp;
}

=begin
Begin-Doc
Name: debug
Access: private
Syntax: $x = $obj->debug()
Syntax: $obj->debug($newval);
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
Name: db
Access: private
Type: method
Comments: Returns db handle
Syntax: $x = $obj->db()
Syntax: $obj->db($newval);
End-Doc
=cut

sub db {
    my $self = shift;

    if (@_) {
        $self->{db} = shift;
    }
    else {
        return $self->{db};
    }
}

=begin
Begin-Doc
Name: count_workable
Type: method
Description: counts workable entries in a queue
Syntax: $cnt = $obj->count_workable(queue => "queuename", [window => $seconds], [indexhint => $indexname])
End-Doc
=cut

sub count_workable {
    my $self      = shift;
    my %opts      = @_;
    my $queue     = $opts{queue} || croak;
    my $window    = $opts{window} || 120;
    my $tbl       = $self->{table};
    my $indexhint = $opts{indexhint};
    my $db        = $self->db();

    # need to make this dynamic delay for exp backoff

    # ungrabbed and either never grabbed, or previous grab over $window seconds ago
    # grabbed and over window seconds ago
    # (basically, don't retry until window has passed, even if ungrabbed)
    my $qry = "select count(*) from $tbl ";

    if ($indexhint) {
        $qry .= " use index ($indexhint) ";
    }

    $qry .= " where queue=? and 
            ( grabtime is null or (grabtime < date_sub(now(),interval ? second)) ) and
            ( grabbed != 'Y' or (grabbed='Y' and grabtime < date_sub(now(),interval ? second)) ) and 
            queuetime < now()";
    my $cid = $db->SQL_OpenQuery( $qry, $queue, $window, $window ) || $db->SQL_Error($qry) && return undef;
    my ($cnt) = $db->SQL_FetchRow($cid);
    $db->SQL_CloseQuery($cid);

    return $cnt;
}

=begin
Begin-Doc
Name: grab_workable
Type: method
Description: marks workable items in the work queue
Syntax: $cnt = $obj->grab_workable(queue => "queuename", [order => $fieldlist], [window => $seconds], [factor => $max_to_grab], [indexhint => $indexname])
End-Doc
=cut

sub grab_workable {
    my $self      = shift;
    my %opts      = @_;
    my $queue     = $opts{queue} || croak;
    my $window    = $opts{window} || 120;
    my $factor    = $opts{factor} || 1;
    my $tbl       = $self->{table};
    my $shuffle   = $opts{shuffle};
    my $order     = $opts{order};
    my $indexhint = $opts{indexhint};
    my $db        = $self->db();

    my $hn = hostname;

    # ungrabbed and either never grabbed, or previous grab over $window seconds ago
    # grabbed and over window seconds ago
    # (basically, don't retry until window has passed, even if ungrabbed)
    my $qry = "update $tbl ";

    if ($indexhint) {
        $qry .= " use index ($indexhint) ";
    }

    $qry .= "set grabbed='Y',grabhost=?,grabpid=?,grabtime=now(),attempts=attempts+1 where queue=? and 
            ( grabtime is null or (grabtime < date_sub(now(),interval ? second)) ) and
            ( grabbed != 'Y' or (grabbed='Y' and grabtime < date_sub(now(),interval ? second)) ) and 
            queuetime < now()";
    if ($order) {
        $qry .= " order by $order limit ?";
    }
    elsif ($shuffle) {
        $qry .= " order by rand() limit ?";
    }
    else {
        $qry .= " order by queuetime limit ?";
    }
    $db->SQL_ExecQuery( $qry, $hn, $$, $queue, $window, $window, $factor ) || $db->SQL_Error($qry) && return 0;
    my ($cnt) = $db->SQL_RowCount();

    return $cnt;
}

=begin
Begin-Doc
Name: get_marked
Type: method
Description: retrieves work queue items marked for this process
Syntax: $arrayref = $obj->get_marked(queue => "queuename")
Comments: each item returned will be a hash ref, returned in queue time order
End-Doc
=cut

sub get_marked {
    my $self    = shift;
    my %opts    = @_;
    my $queue   = $opts{queue} || croak;
    my $tbl     = $self->{table};
    my $shuffle = $opts{shuffle};
    my $db      = $self->db();
    my $res     = [];

    my $hn = hostname;

    my $qry = "select itemid,meta,queuetime,queue,grabhost,grabpid,attempts from $tbl
        where grabhost=? and grabpid=? and queue=? ";
    if ($shuffle) {
        $qry .= " order by rand()";
    }
    else {
        $qry .= " order by attempts desc,queuetime";
    }
    my $cid = $db->SQL_OpenQuery( $qry, $hn, $$, $queue ) || $db->SQL_Error($qry) && next;
    while ( my ( $itemid, $meta, $queuetime, $qqueue, $grabhost, $grabpid, $attempts ) = $db->SQL_FetchRow($cid) ) {
        push(
            @$res,
            {   itemid    => $itemid,
                meta      => $meta,
                queuetime => $queuetime,
                queue     => $qqueue,
                grabhost  => $grabhost,
                grabpid   => $grabpid,
                attempts  => $attempts,
            }
        );
    }
    $db->SQL_CloseQuery($cid);

    return $res;
}

=begin
Begin-Doc
Name: delete_item
Type: method
Description: removes an item from the queue, and passes in item reference
Syntax: $arrayref = $obj->delete_item($itemref)
Comments: will only remove from the queue if it hasn't been re-queued while being worked
End-Doc
=cut

sub delete_item {
    my $self = shift;
    my $item = shift;
    my $tbl  = $self->{table};
    my $db   = $self->db();
    my $rc   = 0;

    my @errs;

    my $dqry = "delete from $tbl where queue=? and itemid=? and queuetime=?";
    $db->SQL_ExecQuery( $dqry, $item->{queue}, $item->{itemid}, $item->{queuetime} );

    my $err = $db->SQL_ErrorString();
    $rc += $db->SQL_RowCount();

    if ( $db->SQL_ErrorCode() && $db->SQL_ErrorString() =~ /Deadlock/ ) {
        sleep(0.1);

        $db->SQL_ExecQuery( $dqry, $item->{queue}, $item->{itemid}, $item->{queuetime} );
        if ( $db->SQL_ErrorCode() ) {
            $db->SQL_Error($dqry) && return undef;
        }

        $rc += $db->SQL_RowCount();
    }

    return $rc;
}

=begin
Begin-Doc
Name: release_item
Type: method
Description: ungrabs an item in the work queue
Syntax: $arrayref = $obj->release_item($itemref)
End-Doc
=cut

sub release_item {
    my $self = shift;
    my $item = shift;
    my $tbl  = $self->{table};
    my $db   = $self->db();

    my $dqry = "update $tbl set grabbed='N',grabhost=null,grabpid=null where 
                queue=? and itemid=? and grabhost=? and grabpid=?";
    $db->SQL_ExecQuery( $dqry, $item->{queue}, $item->{itemid}, $item->{grabhost}, $item->{grabpid} )
        || $db->SQL_Error($dqry) && return undef;
    my $rc = $db->SQL_RowCount();

    return $rc;
}

=begin
Begin-Doc
Name: extend_grab
Type: method
Description: extends grab of an item in the work queue
Syntax: $arrayref = $obj->extend_grab($itemref)
End-Doc
=cut

sub extend_grab {
    my $self = shift;
    my $item = shift;
    my $tbl  = $self->{table};
    my $db   = $self->db();

    my $dqry = "update $tbl set grabtime=now() where
                queue=? and itemid=? and grabhost=? and grabpid=?";
    $db->SQL_ExecQuery( $dqry, $item->{queue}, $item->{itemid}, $item->{grabhost}, $item->{grabpid} )
        || $db->SQL_Error($dqry) && return undef;
    my $rc = $db->SQL_RowCount();

    return $rc;
}

=begin
Begin-Doc
Name: add
Type: method
Description: adds or updates an item in the queue
Syntax: $res = $obj->add(queue => $queue, itemid => $itemid, meta => $metadata, [delay => $true_false])
End-Doc
=cut

sub add {
    my $self = shift;
    my %opts = @_;
    my $tbl  = $self->{table};
    my $db   = $self->db();

    my $id    = $opts{itemid} || return "missing item id";
    my $queue = $opts{queue}  || return "missing queue";
    my $meta  = $opts{meta};
    my $delay = $opts{delay};
    if ( ref($meta) ) {
        $meta = encode_json($meta);
    }

    if ( !$delay ) {
        my $qry
            = "insert into $tbl (queue,itemid,meta,queuetime) values (?,?,?,now()) on duplicate key update queuetime=now(),meta=?";
        $db->SQL_ExecQuery( $qry, $queue, $id, $meta, $meta )
            || $db->SQL_Error($qry) && return "insert failed";
    }
    else {
        my $qry
            = "insert into $tbl (queue,itemid,meta,queuetime,grabtime) values (?,?,?,now(),now()) on duplicate key update queuetime=now(),grabtime=now(),meta=?";
        $db->SQL_ExecQuery( $qry, $queue, $id, $meta, $meta )
            || $db->SQL_Error($qry) && return "insert failed";
    }

    return undef;
}

=begin
Begin-Doc
Name: exists
Type: method
Description: returns if an item id is already inn the queue
Comments: This is obviously polling and not atomic for other operations but good for quick checks
Syntax: $found = $obj->exists(queue => $queue, itemid => $itemid);
End-Doc
=cut

sub exists {
    my $self = shift;
    my %opts = @_;
    my $tbl  = $self->{table};
    my $db   = $self->db();

    my $id    = $opts{itemid} || return "missing item id";
    my $queue = $opts{queue}  || return "missing queue";

    my $qry = "select count(*) from $tbl where queue=? and itemid=?";
    my ($cnt) = $db->SQL_DoQuery( $qry, $queue, $id );
    return $cnt;
}

1;

