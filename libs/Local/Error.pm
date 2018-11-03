#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#

=begin

Begin-Doc
Name: Local::Error
Type: module
Description: Common error handling/reporting routines
Comments: This module is standalone and does not have any external dependencies outside of a standard perl build.
Comments: Module is a singleton. Multiple creations of the object will always return the same reference.
Comments: 

This module is designed to be used as a common error handling infrastructure across a large
range of library modules. It is designed to stack errors so that an error condition can 
percolate up from a lower level routine, without requiring the author or developer to 
go to special effort to manage the return values of the various routines. When using
this module, standard practice is to ->clear prior to calling any library subroutine,
and then ->check after the subroutine returns. If ->check returns that an error has 
occurred, the caller should either deal with the error condition, or ->set a new error
and pass it further on up the call chain.

See the examples below for details on how the object can be used.

Example:

$error = new Local::Error;

sub insert_record {
	...
	$error->clear();
	$db->SQL_ExecQuery(...) || 
		$error->set("sql error", code => &SQL_ErrorCode() ) && return;
	...
}

sub create_new_entry {
	...
	$error->clear();
	&insert_record(someparm => $somevalue, ....);
	
	if ( $error->check() ) {
		print "failed to create entry";
		$error->set("failed to create entry", someparm => $somevalue);
		return;
	}

	# do some other stuff
}


&create_new_entry();


End-Doc

=cut

package Local::Error;
require Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

use Data::Dumper;
use Local::UsageLogger;

@ISA    = qw(Exporter);
@EXPORT = qw();

BEGIN {
    &LogAPIUsage();
}

our $obj = undef;

# Begin-Doc
# Name: new
# Type: function
# Description: Creates object
# Syntax: $maint = new Local::Error()
# End-Doc
sub new {
    my $self = shift;
    my $class = ref($self) || $self;

    if ( !$obj ) {
        $obj = bless {}, $class;
    }

    return $obj;
}

# Begin-Doc
# Name: set
# Type: function
# Description: Sets an error condition
# Syntax: $obj->set($msg, [%data])
# End-Doc
sub set {
    my $self = shift;
    my $msg  = shift;
    my %data = @_;
    my @tmp  = %data;

    my $entry = {};
    if ( $#tmp >= 0 ) {
        $entry->{data} = {%data};
    }
    $entry->{msg} = $msg;
    $entry->{"caller"} = ( caller(1) )[3];
    $entry->{"caller_location"} = ( caller(1) )[1] . ":" . ( caller(1) )[2];

    unshift( @{ $self->{stack} }, $entry );
}

# Begin-Doc
# Name: clear
# Type: function
# Description: Sets an error condition
# Syntax: $obj->clear();
# End-Doc
sub clear {
    my $self = shift;
    $self->{stack} = undef;
}

# Begin-Doc
# Name: check
# Type: function
# Description: Checks if error condition is set
# Syntax: $obj->check()
# Comments: if any data passed, only returns true if at least one record has matches every specified field
# End-Doc
sub check {
    my $self = shift;
    my %data = @_;

    if ( $self->{stack} ) {
        return 1;
    }
    return 0;
}

# Begin-Doc
# Name: get
# Type: function
# Description: returns contents of error stack or undef
# Syntax: @stack = $obj->get()
# Comments: similar to check, but returns data
# Returns: array of hashes with keys 'data', 'msg', 'caller', 'caller_file'. Last entry in array is lowest level function.
# End-Doc
sub get {
    my $self = shift;
    return $self->{stack};
}

# Begin-Doc
# Name: check_data
# Type: function
# Description: Checks if a particular data element was set in any of the errors
# Syntax: $obj->check_data(%data)
# Comments: returns true if at least one record has matches every specified field in %data
# End-Doc
sub check_data {
    my $self = shift;
    my %data = @_;

    if ( !$self->{data} ) {
        return 0;
    }

    my @keys = keys(%data);
    if ( $#keys < 0 ) {
        return 0;
    }

    foreach my $rref ( @{ $self->{stack} } ) {
        my %rdata = %{ $rref->{data} };

        my $ok  = 0;
        my $chk = 0;

        foreach my $key ( keys(%data) ) {
            if ( $rdata{$key} eq $data{$key} ) {
                $ok++;
            }
            $chk++;
        }

        if ( $ok > 0 && $ok == $chk ) {
            return 1;
        }
    }

    return 0;
}

# Begin-Doc
# Name: format
# Type: function
# Description: returns a formatted error message
# Syntax: $txt = $obj->format([%opts]);
# Comments: %opts has key 'style', which defaults to 'auto'. Can also be set to 'html' or 'text' to force it
# End-Doc
sub format {
    my $self  = shift;
    my %opts  = @_;
    my $style = $opts{style} || "auto";

    if ( $style eq "auto" ) {
        if ( $ENV{REQUEST_METHOD} ) {
            $style = "html";
        }
        else {
            $style = "text";
        }
    }

    if ( $style eq "html" ) {
        return $self->as_html();
    }
    else {
        return $self->as_text();
    }
}

# Begin-Doc
# Name: as_text
# Type: function
# Description: returned a formatted error as plain text
# Syntax: $txt = $obj->as_text();
# End-Doc
sub as_text {
    my $self = shift;

    my $text = "";

    $text .= Dumper( $self->{stack} ) . "\n";

    return $text;
}

# Begin-Doc
# Name: as_html
# Type: function
# Description: returned a formatted error as plain text
# Syntax: $txt = $obj->as_html();
# End-Doc
sub as_html {
    my $self = shift;

    my $text = "";

    $text .= "<PRE>\n" . Dumper( $self->{stack} ) . "\n</PRE>\n";

    return $text;
}

# Begin-Doc
# Name: check_and_die
# Type: function
# Description: if error is set, output error message and terminate
# Syntax: $obj->check_and_die([%opts]);
# Comments: %opts can have parameter email, which contains an email address to mail this error message to
# Comments: Ideally, this routine should never be used in a library, since a properly behaved library
#  routine should never terminate the main program.
# End-Doc
sub check_and_die {
    my $self = shift;
    my %opts = @_;

    if ( $self->check ) {
        if ( $ENV{REQUEST_METHOD} ) {
            print "<P>\n";
            print "<B>Terminating process due to error condition:\n";
            print "<P>\n";
        }
        else {
            print "\n\n";
            print "Terminating process due to error condition:\n\n";
        }
        print $self->format();

        if ( $opts{email} ne "" ) {
            open( my $ErrMail, "|-" )
                || exec( "/usr/lib/sendmail", $opts{email} );
            print $ErrMail "Subject: Local::Error error detected.\n";
            print $ErrMail "To: ", $opts{email}, "\n";
            print $ErrMail "\n\n";
            print $ErrMail $self->as_text();
            print $ErrMail "\n";
            close($ErrMail);
        }

        die;
    }
}

1;
