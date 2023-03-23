package Local::RQueue;
require Exporter;

use Redis::Fast;
use JSON;

use strict;

# Begin-Doc
# Name: RQueue
# Type: module
# Description: Module for work queue with redis backing
# Syntax:  use RQueue
# End-Doc

use vars qw(@ISA @EXPORT);
@ISA    = qw(Exporter);
@EXPORT = qw();

# Begin-Doc
# Name: new
# Type: function
# Description:  establishes object
# Syntax: $ex = new Local::RQueue()
# End-Doc
sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;

    # set any object params
    my $tmp = {};

    $tmp->{"debug"} = $opts{debug};

    # Bless the object
    bless $tmp, $class;

    return $tmp;
}

# Begin-Doc
# Name: debug
# Type: method
# Access: public
# Description: Sets or returns current module debugging level
# Syntax: $obj->debug(1) to enable
# Syntax: $obj->debug(0) to disable
# End-Doc
sub debug {
    my $self = shift;
    if (@_) {
        $self->{debug} = shift;
    }
    else {
        return $self->{debug};
    }
}

# Begin-Doc
# Name: error
# Type: method
# Access: public
# Description: Sets or returns current object error status
# Syntax: $obj->error() to retrieve
# Syntax: $obj->error(undef) to clear
# Syntax: $obj->error(msg) to set
# End-Doc
sub error {
    my $self = shift;
    if ( scalar(@_) ) {
        $self->{error} = shift;
    }
    else {
        return $self->{error};
    }
}

=begin
Begin-Doc
Name: redis
Type: method
Description: returns a handle to Redis
Syntax: $redis = $obj->redis();
End-Doc
=cut

sub redis {
    my $self = shift;

    if ( !$self->{redis} ) {

        # For time being code only for a local redis instance accessed through loopback
        # Add parameter support in future

        my $redis = Redis::Fast->new( server => "127.0.0.1:6379", reconnect => 1 );
        $self->{redis} = $redis;
    }

    return $self->{redis};
}

=begin
Begin-Doc
Name: add
Type: method
Description: inserts an entry into a queue
Syntax: $res = $obj->add(queue => "name", itemid => "id", meta => "metastring");
End-Doc
=cut

sub add {
    my $self   = shift;
    my %opts   = @_;
    my $queue  = $opts{queue} || "default";
    my $itemid = $opts{itemid} || return "missing item id";
    my $meta   = $opts{meta} || "{}";

    my $redis = $self->redis();

    my $hash_pending = "q_pending_${queue}";
    my $hash_working = "q_working_${queue}";
    my $hash_meta    = "q_meta_${queue}";

    my $lua = <<EOF;
local q_pending = KEYS[1]
local q_working = KEYS[2]
local q_meta = KEYS[3]

local itemid = ARGV[1]
local meta = ARGV[2]
local result = ""
local newver = 0

if(redis.call('HEXISTS', q_working, itemid) == 1) then
    newver = redis.call('HINCRBY', q_working, itemid, 1)
else
    -- will insert with val 0 and increment if not already in hash
    newver = redis.call('HINCRBY', q_pending, itemid, 1)
end

-- maybe questionable, but always update metadata
redis.call('HSET', q_meta, itemid, meta)

return newver
EOF
    my $res = $redis->eval( $lua, 3, $hash_pending, $hash_working, $hash_meta, $itemid, $meta );

    return $res;
}

=begin
Begin-Doc
Name: grab
Type: method
Description: grabs an entry from work queue, return ref to item
Syntax: $itemref = $obj->grab(queue => "name", [window => 60]);
End-Doc
=cut

sub grab {
    my $self   = shift;
    my %opts   = @_;
    my $queue  = $opts{queue} || "default";
    my $window = $opts{window} || 60;

    my $redis = $self->redis();

    my $hash_pending = "q_pending_${queue}";
    my $hash_working = "q_working_${queue}";
    my $hash_meta    = "q_meta_${queue}";
    my $hash_expires = "q_expires_${queue}";

    my $lua = <<EOF;
local q_pending = KEYS[1]
local q_working = KEYS[2]
local q_meta = KEYS[3]
local q_expires = KEYS[4]

local tstamp = ARGV[1]

local itemid = redis.call('HRANDFIELD', q_pending)
if (itemid) then
    local ver = redis.call('HGET', q_pending, itemid)
    redis.call('HSET', q_working, itemid, ver)
    redis.call('HDEL', q_pending, itemid)
    redis.call('HSET', q_expires, itemid, tstamp)
    return itemid
else
    return nil
end
EOF
    my $itemid
        = $redis->eval( $lua, 4, $hash_pending, $hash_working, $hash_meta, $hash_expires, int( time + $window ) );
    if ($itemid) {
        my $ver  = $redis->hget( $hash_working, $itemid );
        my $meta = $redis->hget( $hash_meta,    $itemid );
        return {
            id      => $itemid,
            queue   => $queue,
            itemid  => $itemid,
            version => $ver,
            meta    => $meta
        };
    }
    else {
        return undef;
    }
}

=begin
Begin-Doc
Name: extend_grab
Type: method
Description: extends expiration of grab of an entry in working queue
Syntax: $obj->extend_grab($itemref, [window => 60])
End-Doc
=cut

sub extend_grab {
    my $self   = shift;
    my $iref   = shift;
    my %opts   = @_;
    my $queue  = $iref->{queue} || return undef;
    my $window = $opts{window} || 60;

    my $redis = $self->redis();

    my $hash_pending = "q_pending_${queue}";
    my $hash_working = "q_working_${queue}";
    my $hash_meta    = "q_meta_${queue}";
    my $hash_expires = "q_expires_${queue}";

    my $lua = <<EOF;
local q_pending = KEYS[1]
local q_working = KEYS[2]
local q_meta = KEYS[3]
local q_expires = KEYS[4]

local itemid = ARGV[1]
local ver = ARGV[2]
local tstamp = ARGV[3]

local found_ver = redis.call('HGET', q_working, itemid)
if (found_ver == ver) then
    redis.call('HSET', q_expires, itemid, tstamp)
    return itemid
else
    return nil
end
EOF
    my $itemid = $redis->eval(
        $lua,       4,             $hash_pending, $hash_working,
        $hash_meta, $hash_expires, $iref->{id},   $iref->{version},
        int( time + $window )
    );
    if ($itemid) {
        return $iref;
    }
    else {
        return undef;
    }
}

=begin
Begin-Doc
Name: release_item
Type: method
Description: release an item and put back in pending queue
Syntax: $res = $obj->release_item($iref);
End-Doc
=cut

sub release_item {
    my $self  = shift;
    my $iref  = shift;
    my $queue = $iref->{queue} || return undef;

    my $redis = $self->redis();

    my $hash_pending = "q_pending_${queue}";
    my $hash_working = "q_working_${queue}";
    my $hash_meta    = "q_meta_${queue}";
    my $hash_expires = "q_expires_${queue}";

    my $lua = <<EOF;
local q_pending = KEYS[1]
local q_working = KEYS[2]
local q_meta = KEYS[3]
local q_expires = KEYS[4]

local itemid = ARGV[1]
local ver = ARGV[2]

local found_ver = redis.call('HGET', q_working, itemid)
if (found_ver == ver) then
    redis.call('HDEL', q_working, itemid)
    redis.call('HDEL', q_expires, itemid)

    redis.call('HSET', q_pending, itemid, ver)
    redis.call('HINCRBY', q_pending, itemid, 1)

    return itemid
else
    return nil
end
EOF
    my $res = $redis->eval( $lua, 4, $hash_pending, $hash_working, $hash_meta, $hash_expires, $iref->{id},
        $iref->{version} );
    return $res;
}

=begin
Begin-Doc
Name: delete_item
Type: method
Description: deletes item from working queue entirely
Syntax: $obj->delete_item($itemref)
End-Doc
=cut

sub delete_item {
    my $self  = shift;
    my $iref  = shift;
    my %opts  = @_;
    my $queue = $iref->{queue} || return undef;

    my $redis = $self->redis();

    my $hash_working = "q_working_${queue}";
    my $hash_meta    = "q_meta_${queue}";
    my $hash_expires = "q_expires_${queue}";

    my $lua = <<EOF;
local q_working = KEYS[1]
local q_meta = KEYS[2]
local q_expires = KEYS[3]

redis.call('HDEL', q_working, itemid)
redis.call('HDEL', q_meta, itemid)
redis.call('HDEL', q_expires, itemid)

return nil
EOF
    $redis->eval( $lua, 3, $hash_working, $hash_meta, $hash_expires );
    return undef;
}

=begin
Begin-Doc
Name: pending
Type: method
Description: returns count of pending items
Syntax: $obj->pending(queue => $queue)
End-Doc
=cut

sub pending {
    my $self  = shift;
    my %opts  = @_;
    my $queue = $opts{queue} || return undef;

    my $redis = $self->redis();

    my $hash_pending = "q_pending_${queue}";
    return $redis->hlen($hash_pending);
}

=begin
Begin-Doc
Name: working
Type: method
Description: returns count of working items
Syntax: $obj->working(queue => $queue)
End-Doc
=cut

sub working {
    my $self  = shift;
    my %opts  = @_;
    my $queue = $opts{queue} || return undef;

    my $redis = $self->redis();

    my $hash_working = "q_working_${queue}";
    return $redis->hlen($hash_working);
}

# need maintain() -> call lua - iterate through 'working', moving back to pending with a version increment if past expiration time
# should be called periodically, likely in it's own scheduled task, might also do something where this is called directly from the
# module periodically with a redis key to keep track of the last time the clean pass was run for concurrency purposes to make it a no-op

1;
