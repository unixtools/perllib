#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
#

=begin
Begin-Doc
Name: Local::ParallelRun
Type: module
Description: an object that provides a parallel execution facility with maximum job limiting
Comments: This is an object that allows the execution of a list of commands,
running up to a certain number of them in parallel. This can allow
the complete list of commands to complete execution much faster than
executing them sequentially.

Note - be careful using this module in conjunction with connections to Oracle, as the Oracle
libraries/API do not handle forks very well and you will lose your database connection.
End-Doc
=cut

package Local::ParallelRun;
use Exporter;
use strict;
use Local::UsageLogger;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw();

BEGIN {
    &LogAPIUsage();
}

#
# to avoid fork stdout weirdness
#
$| = 1;

# Begin-Doc
# Name: new
# Type: method
# Description: create a new ParallelRun object
# Syntax: $obj = new ParallelRun(%opts)
# Comments: options can be any of the following
# Comments: timeout => number of seconds to wait for job completion
# Comments: maxjobs => maximum number of jobs to run in parallel
# Comments: debug => enable debug output if nonzero
# End-Doc
sub new {
    my $self   = shift;
    my $class  = ref($self) || $self;
    my (%opts) = @_;

    my $tmp = {};
    $tmp->{timeout} = $opts{timeout} || 60;
    $tmp->{maxjobs} = $opts{maxjobs} || 10;
    $tmp->{jobs}    = {};
    $tmp->{jobstate}   = {};
    $tmp->{pid_to_job} = {};
    $tmp->{tags}       = [];
    $tmp->{tag}        = 0;                   # last tag value used
    $tmp->{debug}      = $opts{debug} || 0;

    &LogAPIUsage();

    return bless $tmp, $class;
}

#
# Constants
#
my $STATE_READY   = 0;
my $STATE_RUNNING = 1;
my $STATE_TIMEOUT = 2;
my $STATE_DONE    = 3;

# Begin-Doc
# Name: add
# Type: method
# Description: add a job to run
# Syntax: $tag = $obj->add(cmd => $cmd, [timeout => $seconds,]
#	[stdout => 'filename',] [stdin=>'filename',] [stderr=>'filename',]);
# Comments: Adds a job to the queue, input/output files are closed by default.
# 	It is ok to specify the same file for stdout and stderr.
# Access: public
# End-Doc
sub add {
    my $self = shift;
    my (%info) = @_;
    my ( $job, $tag, $field );

    $self->{tag}++;
    $tag = $self->{tag};

    if ( !defined( $info{cmd} ) ) {
        return 0;
    }

    $job = {};
    foreach $field (qw|cmd timeout stdin stdout stderr|) {
        $job->{$field} = $info{$field};
    }

    $self->{jobs}->{$tag}     = $job;
    $self->{jobstate}->{$tag} = $STATE_READY;
    push( @{ $self->{tags} }, $tag );

    $self->{debug} && print "added task with tag $tag\n";

    return $tag;
}

#
# fetch - returns info for a particular job
#

#
# process - one iteration of checking for child status and starting
# new processes (internal)
#
sub _process {

}

#
# status - returns info for the object
#

# Begin-Doc
# Name: _countstate
# Type: method
# Description: return the count of how many jobs are running
# Access: internal
# End-Doc
sub _countstate {
    my $self  = shift;
    my $state = shift;
    my ( $count, $tag );

    $count = 0;
    foreach $tag ( @{ $self->{tags} } ) {
        if ( $self->{jobstate}->{$tag} == $state ) {
            $count++;
        }
    }

    $self->{debug} && print "$count tasks in state $state\n";
    return $count;
}

# Begin-Doc
# Name: _startjob
# Type: method
# Description: fork off a new job
# Access: internal
# End-Doc
sub _startjob {
    my $self = shift;
    my ( $childpid, $savefh, $i, $tag, $tmptag );
    my ( $state, %tmptags );

    $self->{debug} && print "_startjob:\n";

    # Determine which job to run
    $tag = 0;

    # stick tags in a hash to pseudo-randomize them
    # this way jobs don't run in sequential order always
    # - we want this because otherwise, when adding a list of jobs for
    # a particular host, it is less likely to have LOTS of jobs running
    # on one host, which defeats the purpose of running in parallel in
    # that particular setup - and it drives that one host too hard.
    %tmptags = ();
    foreach $tmptag ( @{ $self->{tags} } ) {
        $tmptags{$tmptag} = 1;
    }
    foreach $tmptag ( keys(%tmptags) ) {
        $state = $self->{jobstate}->{$tmptag};
        if ( $state == $STATE_READY ) {
            $tag = $tmptag;
            last;
        }
    }

    if ( $tag == 0 ) {
        die "No job to run. ICK, shouldn't get here.\n";
    }

    $self->{jobstate}->{$tag} = $STATE_RUNNING;

    my %job = %{ $self->{jobs}->{$tag} };
    $childpid = fork;
    if ( $childpid == 0 )    # child
    {
        $self->{debug} && print "In Child($$)\n";

        #
        # set up timeout handler
        #
        $SIG{ALRM} = sub { exit(254); };
        if ( $job{timeout} ) {
            $self->{debug} && print "$$: timeout=" . $job{timeout} . "\n";
            alarm( $job{timeout} );
        }
        else {
            $self->{debug} && print "$$: timeout=" . $self->{timeout}, "\n";
            alarm( $self->{timeout} );
        }

        #
        # Dump the job
        #
        if ( $self->{debug} ) {
            print "\n\n";
            while ( my ( $k, $v ) = each %job ) {
                print "\t$k => $v\n";
            }
        }

        #
        # Set up output/input
        #
        open( SAVE_STDOUT, ">&STDOUT" );
        close(STDOUT);
        close(STDIN);
        close(STDERR);

        if ( defined( $job{stdin} ) ) {
            $self->{debug} && print "$$: stdin => $job{stdin}\n";
            open( STDOUT, "<" . $job{stdin} );
        }

        if ( defined( $job{stdout} ) ) {
            if ( $job{stdout} eq "-" ) {
                open( STDOUT, ">&SAVE_STDOUT" );
                $self->{debug} && print "$$: stdout => real stdout\n";
            }
            else {
                open( STDOUT, ">" . $job{stdout} );
                $self->{debug} && print "$$: stdout => $job{stdout}\n";
            }
        }

        if ( $job{stderr} eq $job{stdout} ) {
            $self->{debug} && print "$$: stderr => stdout\n";
            open( STDERR, ">&STDOUT" );
        }
        elsif ( defined( $job{stderr} ) ) {
            if ( $job{stderr} eq "-" ) {
                $self->{debug} && print "$$: stderr => real stdout\n";
                open( STDERR, ">&SAVE_STDOUT" );
            }
            else {
                $self->{debug} && print "$$: stderr => $job{stderr}\n";
                open( STDERR, ">" . $job{stderr} );
            }
        }

        select(STDIN);
        $| = 1;
        select(STDERR);
        $| = 1;
        select(STDOUT);
        $| = 1;

        $self->{debug} && print "$$: start\n";
        $self->{debug} && print "$$: cmd='$job{cmd}'\n";
        system( $job{cmd} );
        my $ret   = $?;
        my $res   = $ret >> 8;
        my $dcore = $ret & 127;
        my $sig   = $ret & 128;
        $self->{debug}
            && print "$$: return=$ret res=$res dcore=$dcore sig=$sig\n";
        $self->{debug} && print "$$: done\n";
        exit( $? >> 8 );
    }
    elsif ( $childpid > 0 )    # parent
    {
        $self->{pid_to_job}->{$childpid} = $tag;
    }
    else {
        die "couldn't fork!\n";

        # fork err
    }
}

# Begin-Doc
# Name: run
# Type: method
# Description: the main loop, runs jobs
# Comment: starts new jobs when a slot is available, runs until the idle
# 	timeout has passed or all jobs have completed.
# Access: public
# End-Doc
sub run {
    my $self = shift;
    my ( $done, $newjob );

    $done = 0;
    while ( !$done ) {
        $self->{debug} && print "Loop\n";

        #
        # Start new jobs if slots available
        #
        while (( $self->_countstate($STATE_RUNNING) < $self->{maxjobs} )
            && ( $self->_countstate($STATE_READY) > 0 ) )
        {
            $self->{debug} && print "Starting a new job.\n";
            $self->_startjob;
        }

        #
        # Reap any children which have quit
        #
        $self->{debug} && print "Waiting.\n";
        my $wait_pid = wait;
        if ( $wait_pid != -1 ) {
            my $wait_exitstatus = $? >> 8;
            my $wait_sigstatus  = $? & 127;
            my $wait_corestatus = $? & 128;

            $self->{debug}
                && print "Pid ($wait_pid) died with status ($wait_exitstatus).\n";
            my $tag = $self->{pid_to_job}->{$wait_pid};
            if ( $tag >= 0 ) {
                $self->{jobstate}->{$tag} = $STATE_DONE;
            }
        }

        #
        # Done if ready==0 and running==0
        #
        if (   ( $self->_countstate($STATE_RUNNING) == 0 )
            && ( $self->_countstate($STATE_READY) == 0 ) )
        {
            $done = 1;
        }

        #
        # delay a little
        #
        select( undef, undef, undef, 0.05 );
    }
}

1;

