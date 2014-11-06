#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#

=begin

Begin-Doc
Name: Local::TSort
Type: module
Description: topological sort
Comments: implements a topological sort w/ special handling for loops, also
has handling for two special types of rules "before everything", and "after everything"
End-Doc

=cut

package Local::TSort;
use Exporter;
use strict;
use Local::UsageLogger;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw(TSort);

BEGIN {
    &LogAPIUsage();
}

#
# This module provides an interface to a topological sorting
# mechanism. This code will perform a tsort as normally found, but
# with the following additional capabilities
#
# 'max' and 'min' special elements - this is usefull for when you want
# certain elements to be as early or as late as possible in the sorted
# list, but still be able to sort within the maximized or minimized
# elements. i.e. in rdist parlance "host-config" and "hostgroup-config"
# should sort after ALL elements, but host-config should sort after
# hostgroup-config
#
# The max and min elements are user-defined, defaulting to 'undef'. i.e.
# a rule "a > undef" will say that a is greater than ALL elements. Any
# rules involving max/min will be compiled into X rules indicating the
# given relation to all other elements other than other elements involving
# the same max or min.
#
# Loop handling - this code will detect loops and remove them. Any
# additional rule that is defined that would cause a loop will immediately
# be skipped.

# Begin-Doc
# Name: TSort
# Type: function
# Description: performs sort according to passed in data
# Syntax: @res = &TSort(%params)
# Comment: %params are any of:
# Comment: rules => ref to array of refs to sorting rules: [a,b] means that
# 	a must come before b in the resultant array
# Comment: items => ref to array of items to sort
# Comment: extrema => defines the min/max element for sorting rules
# Comment: debug => enable debugging
# Comment: This module performs a topological sort given a list of elements,
# a set of rules, and an optional 'extrema' element, which allows order
# items to the beginning or end of the list.
# If you have rules so as "[*,a]" and set "*" to be the extrema element,
# the tsort will try to order "a" as far to the end of the list as
# possible. If you have more than one rule like this, they are applied
# with the first one defined taking the most precedence.
# End-Doc
sub TSort {
    my (%parms) = @_;
    my $rules   = $parms{"rules"}   || [];
    my $items   = $parms{"items"}   || [];
    my $extrema = $parms{"extrema"} || undef;
    my $debug   = $parms{"debug"}   || 0;
    my ( %alist, @res, $item, @nodes, $rule, $low, $high, $key, $found );

    &LogAPIUsage();

    $debug && print "Extrema = $extrema\n";

    foreach $item (@$items) {
        $alist{$item} = {};
        $debug && print "set alist{$item}\n";
    }

    @nodes = keys(%alist);
    foreach $rule (@$rules) {
        ( $low, $high ) = @$rule;
        $debug && print "Rule: '$low'<'$high'\n";

        if ( $high eq $extrema & defined( $alist{$low} ) ) {
            $debug && print "handling high extrema\n";
            foreach $key (@nodes) {

                # not for self
                if ( $key eq $low ) {
                    $debug && print "skipping self for $low<*\n";
                    next;
                }

                # not if already reverse rule defined
                if ( $alist{$low}->{$key} ) {
                    $debug && print "skipping $key for $low<*\n";
                    next;
                }

                $alist{$key}->{$low} = 1;
                $debug && print "set minmax $low < $key\n";
            }
        }
        elsif ( $low eq $extrema & defined( $alist{$high} ) ) {
            $debug && print "handling low extrema\n";
            foreach $key (@nodes) {

                # not for self
                if ( $key eq $high ) {
                    $debug && print "skipping self for $high>*\n";
                    next;
                }

                # not if already reverse rule defined
                if ( $alist{$key}->{$high} ) {
                    $debug && print "skipping $key for $high>*\n";
                    next;
                }

                $alist{$high}->{$key} = 1;
                $debug && print "set minmax $key < $high\n";
            }
        }
        elsif ( !defined( $alist{$high} ) | !defined( $alist{$low} ) ) {
            $debug && print "skipping $low < $high\n";
        }
        else {
            if ( $alist{$low}->{$high} ) {
                $debug && print "skipping $low for $high>*\n";
                next;
            }
            else {
                $alist{$high}->{$low} = 1;
                $debug && print "set $low < $high\n";
            }
        }
    }

    while ( scalar(%alist) ) {
        $found = undef;
        foreach $key ( keys(%alist) ) {
            if ( !scalar( %{ $alist{$key} } ) ) {
                $debug && print "found '$key'\n";
                $found = $key;
                last;
            }
        }
        $debug && print "after foreach\n";

        if ($found) {
            delete( $alist{$found} );
            foreach $key ( keys(%alist) ) {
                delete( $alist{$key}->{$found} );
            }
            $debug && print "did deletes\n";
            push( @res, $found );
        }
        else {
            die "TSort loop detected\n";
        }
    }
    return @res;
}

1;

