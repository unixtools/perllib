#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: http://svn.unixtools.org/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#


=begin

Begin-Doc
Name: Local::SQLTidy
Type: module
Description: Simple/minimal sql reformatting function for pretty printing
Comments: 

End-Doc

=cut

package Local::SQLTidy;
require 5.000;
require Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw( SQLTidy SQLTidyWrapper );
use Text::Balanced qw(extract_delimited extract_tagged gen_extract_tagged);

# Begin-Doc
# Name: SQLTidyWrapper
# Type: function
# Description: Reads from stdin and writes to stdout formatted sql
# Syntax: &SQLTidyWrapper()
# Comments: This is a simple wrapper around SQLTidy to allow use via:
# Comments: perl -I/local/perllib/libs -MLocal::SQLTidy -e SQLTidyWrapper
# End-Doc
sub SQLTidyWrapper {
    my $text = do { local $/; <> };
    my $out = &SQLTidy($text);
    print $out;
}

# Begin-Doc
# Name: SQLTidy
# Type: function
# Description: Returns reformatted version of input sql
# Syntax: ($sqltext,$status) = &SQLTidy($text, %options)
# Syntax: $sqltext = &SQLTidy($text, %options)
# Comments: returns reformatted text and status, if $status is not null, formatting was not successful
# Comments: if called in scalar context, only returns reformatted text
# Comments: available options 'debug' - if nonzero will print out tracing info, 'keep_case' - if nonzero will
#  not force all sql text to lowercase.
# End-Doc
sub SQLTidy {
    my $text = shift;
    my %opts = @_;

    my $origtext = $text;

    my $debug     = $opts{debug};
    my $keep_case = $opts{keep_case};

    my $prefix = "";
    my $suffix = "";

    #
    # First determine initial indent level
    #
    my $initial_indent = "";
    if ( $text =~ /^(\s*[\r\n]+)*(\s*)\S/sgmo ) {

        # Replace tabs with 4 spaces by coding standard
        $initial_indent = $2;
        $initial_indent =~ s/\t/    /go;
        $debug && print "Initial indent: ", length($initial_indent),
            " spaces.\n";
    }

#
# Special handling to allow inline selection of a perl sql query with surrounding quotes
#
    if ( $text =~ /^\s*(")(.*)("\s*;*)\s*/so ) {
        $prefix = $1;
        $text   = $2;
        $suffix = $3;
    }

    #
    # Now, split off the text chunk by chunk
    #
    my @pieces = ();
    while ( $text ne "" ) {
        next if ( $text =~ s/^\s+//sgo );
        next if ( $text =~ s/\s+$//sgo );

        $debug && print "\ntext: $text\n";

        # Find the next chunk up to a delimeter
        # But if this chunk starts with a quote, then handle special
        if ( substr( $text, 0, 1 ) eq "\'" ) {
            my ( $extracted, $remainder, $prefix ) = extract_delimited(
                $text,
                "\'",        # Our chosen delimiter
                "[^\']*",    # Allow for text before the delimiter
                "\'"
            );               # Escape delimiter when doubled

            $debug && print "got sq chunk ($extracted)\n";
            push( @pieces, $extracted );
            $text = $remainder;
        }
        elsif ( substr( $text, 0, 1 ) eq "\"" ) {
            my ( $extracted, $remainder, $prefix ) = extract_delimited(
                $text,
                "\"",        # Our chosen delimiter
                "[^\"]*",    # Allow for text before the delimiter
                "\""
            );               # Escape delimiter when doubled

            $debug && print "got dq chunk ($extracted)\n";
            push( @pieces, $extracted );
            $text = $remainder;
        }
        elsif ( $text =~ m|\A(\-\-.*)\s*$|mo ) {
            my $chunk = $1;
            push( @pieces, $chunk );
            $debug && print "got dash comment ($chunk)\n";
            $text = substr( $text, length($chunk) );
        }
        elsif ( $text =~ m|^(/\*.*?\*/)|so ) {
            my $chunk = $1;

            push( @pieces, $chunk );
            $debug && print "got c-style comment ($chunk)\n";
            $text = substr( $text, length($chunk) );
        }
        elsif ( $text =~ m|^([^,\'\"\(\)\s\=\+\-\*\/<\>]+)$|so ) {

            # vim-hilite: "'

            $text = "";

            if ($keep_case) {
                push( @pieces, $1 );
            }
            else {
                push( @pieces, lc $1 );
            }
        }
        else {
            if ( $text =~ m|^(.*?)([,\'\"\(\)\s\=\+\-\*\/<\>])|smo ) {

                # vim-hilite: "'

                my $chunk1 = $1;
                my $chunk2 = $2;

                unless ($keep_case) {
                    $chunk1 = lc $chunk1;
                    $chunk2 = lc $chunk2;
                }

                if ( $chunk1 ne "" ) {
                    $debug
                        && print "stripping "
                        . length($chunk1)
                        . " bytes ($chunk1) off text (ch1).\n";
                    $text = substr( $text, length($chunk1) );
                    $debug && print "text now: $text\n";
                }

                if ( $chunk1 !~ /^\s*$/o ) {

                    $debug && print "got chunk1 ($chunk1)\n";
                    push( @pieces, $chunk1 );
                }

                if ( $chunk2 !~ /^[\'\"]/ ) {
                    $debug
                        && print "stripping "
                        . length($chunk2)
                        . " bytes ($chunk2) off text (ch2).\n";
                    if ( $chunk2 ne "" ) {
                        $text = substr( $text, length($chunk2) );
                    }
                    if ( $chunk2 !~ /^\s*$/o ) {

                        $debug && print "got chunk2 ($chunk2)\n";
                        push( @pieces, $chunk2 );
                    }
                }
            }
            else {
                if (wantarray) {
                    return ( $origtext,
                        "unable to pull off chunk. CurText($text)" );
                }
                else {
                    return $origtext;
                }
            }
        }
    }

    #
    # Dump out the pieces
    #
    for ( my $i = 0; $i <= $#pieces; $i++ ) {
        $debug && print "$i: ", $pieces[$i], "\n";
    }

    #
    # Now run through formatting rules and print it out
    #
    my $cur_indent   = 0;
    my $cur_line     = "";
    my @outlines     = ();
    my @paren_levels = ();
    while (@pieces) {
        my $p           = shift @pieces;
        my $next1       = $pieces[0];
        my $next2       = $pieces[1];
        my $this_indent = "    " x $cur_indent;

        # Clump tuples
        my $sp_two    = $p . " " . $next1;
        my $nsp_two   = $p . $next1;
        my $sp_three  = $p . " " . $next1 . " " . $next2;
        my $nsp_three = $p . $next1 . $next2;

        # Handle any clumping
        if ( $sp_two =~ m{^(order by|group by|partition by)$}i ) {
            $p = $sp_two;
            shift @pieces;
        }
        elsif ($nsp_two eq "<="
            || $nsp_two eq ">="
            || $nsp_two eq "()"
            || $nsp_two eq "!="
            || $nsp_two eq "<>" )
        {
            $p = $nsp_two;
            shift @pieces;
        }
        elsif ( $nsp_three =~ /^\(.*\)$/o ) {
            $p = $nsp_three;
            shift @pieces;
            shift @pieces;
        }
        elsif ( $nsp_three =~ /^rank\(\)$/o ) {
            $p = $nsp_three;
            shift @pieces;
            shift @pieces;
        }

        my $lp = lc $p;

        # Proceed with normal processing
        if ( $p eq "(+)" || $p eq "()" ) {

            # push tight with field
            $cur_line .= $p;
        }
        elsif ( $p =~ /^--/ ) {
            $cur_line =~ s/^\s*//go;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_line = $p;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_line = "";
        }
        elsif ( $p =~ m|^/\*| ) {
            $cur_line =~ s/^\s*//go;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_line = $p;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_line = "";
        }
        elsif ($lp eq "distinct"
            || $lp eq "unique"
            || $lp eq "decode"
            || $lp eq "substr"
            || $lp eq "translate"
            || $lp eq "to_number"
            || $lp eq "to_date"
            || $lp eq "to_char"
            || $lp eq "rank" )
        {
            $cur_line .= " $p";
            $cur_line =~ s/^\s*//go;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_line = "";
        }
        elsif ( $p eq "(" ) {
            $cur_line .= " $p";
            $cur_line =~ s/^\s*//go;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_line = "";

            push( @paren_levels, $cur_indent );
            $cur_indent++;
        }
        elsif ( $p eq ")" ) {
            $cur_line =~ s/^\s*//go;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_line = "";

            $cur_indent = pop @paren_levels;
            if ( $cur_indent eq "" ) {

                # we popped all paren levels, probably invalid sql
            }
            $cur_line = $p;
        }
        elsif ( $p eq "," ) {
            $cur_line .= $p;
            $cur_line =~ s/^\s*//go;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_line = "";
        }
        elsif ( $lp =~ m{^(and|or|when)$} ) {
            $cur_line =~ s/^\s*//go;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_line = $p;
        }
        elsif ( $lp =~ m{^(select|case|then)$} ) {
            $cur_line =~ s/^\s*//go;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_line = $p;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_line = "";
            $cur_indent++;
        }
        elsif ( $lp
            =~ m{^(from|else|where|order by|group by|having|union|minus)$} )
        {
            $cur_line =~ s/^\s*//go;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_indent--;
            $this_indent = "    " x $cur_indent;
            $cur_line    = $p;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_line = "";
            $cur_indent++;
        }
        elsif ( $lp =~ m{^(end)$} ) {
            $cur_line =~ s/^\s*//go;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_indent -= 2;
            $this_indent = "    " x $cur_indent;
            $cur_line    = $p;
            push( @outlines, "${initial_indent}${this_indent}${cur_line}" );
            $cur_line = "";
        }
        elsif ( $p =~ m|^[\-\+\=\*\/]$| ) {
            $cur_line .= " $p";
        }
        else {
            my $last_line = $outlines[$#outlines];

            if (   $cur_line eq ""
                && $last_line !~ /^\s*\),\s*$/o
                && $last_line =~ /\,$/o
                && length($last_line) < 60
                && ( length($last_line) + length($p) ) < 65 )
            {
                $cur_line = $last_line;
                pop(@outlines);
            }
            $cur_line .= " $p";
        }
    }

    my $this_indent = "    " x $cur_indent;
    $cur_line =~ s/^\s*//go;
    push( @outlines, "${initial_indent}${this_indent}${cur_line}" );

    #
    # Dump output
    #
    my $outtext;
    if ($prefix) {
        $outtext .= $initial_indent . $prefix . "\n";
    }
    foreach my $line (@outlines) {
        if ( $line !~ /^\s*$/o ) {
            $outtext .= $line . "\n";
        }
    }
    if ($suffix) {
        $outtext .= $initial_indent . $suffix;
    }

    #
    # Perform a double check to make sure no non-whitespace data was changed
    #
    my $orig_data = $origtext;
    $orig_data =~ s/\s*//sgmo;
    unless ($keep_case) {
        $orig_data = lc $orig_data;
    }
    my $new_data = $outtext;
    $new_data =~ s/\s*//sgmo;
    unless ($keep_case) {
        $new_data = lc $new_data;
    }

    my $status = "";

    if ( $orig_data ne $new_data ) {
        $outtext = $origtext;
        $status
            = "failed data comparison, failsafe return\n\n$orig_data\n\nvs\n\n$new_data";
    }

    if (wantarray) {
        return ( $outtext, $status );
    }
    else {
        return $outtext;
    }
}

1;
