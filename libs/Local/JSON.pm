#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
#

=begin

Begin-Doc
Name: Local::JSON
Type: module
Description: superclass of JSON module with support for Local::JSON::Raw() to allow encoding raw javascript in the object

Comments: 

Use just like the object form of the upstream JSON module, with the addition that you pass a Local::JSON::Raw("string") as one of the
elements in the content passed to $obj->encode() to have it output as-is with no quoting.

End-Doc

=cut

package Local::JSON;
use Exporter;
use JSON;
use MIME::Base64;
use strict;

use vars qw (@ISA @EXPORT);
@ISA    = qw(JSON Exporter);
@EXPORT = qw();

our $used_raw = 0;
our $wrapper;

# Begin-Doc
# Name: encode
# Description: wrapper function that calls our translation for Local::JSON::Raw if needed before calling JSON's encode method
# Usage: my $txt = $obj->encode(@vars)
# End-Doc

sub encode {
    my $self = shift;
    my @in   = @_;
    my @out  = ();

    if ( !$used_raw ) {
        return $self->SUPER::encode(@in);
    }
    else {
        if ( !$wrapper ) {
            my @set = ( '0' .. '9', 'A' .. 'F', 'a' .. 'f' );
            $wrapper = join '' => map $set[ rand @set ], 1 .. 60;
            $wrapper = "<<<<<" . $wrapper . ">>>>>";
        }

        my $txt = $self->SUPER::encode( $self->_translate_raw_in( 0, @in ) );
        return $self->_translate_raw_out($txt);
    }
}

# Begin-Doc
# Name: _translate_raw_in
# Description: traverses objects/values and wraps any Local::JSON::Raw elements with wrapper strings
# Usage: my @out = $obj->_translate_raw_out($depth, @in)
# Call with depth of 0
# End-Doc

sub _translate_raw_in {
    my $self  = shift;
    my $depth = shift;
    my @in    = @_;
    my @out   = ();

    foreach my $val (@in) {
        my $ref = ref($val);
        if ( $ref eq "Local::JSON::Raw" ) {
            push( @out, $wrapper . encode_base64($val->{value},'') . $wrapper );
        }
        elsif ( $ref eq "SCALAR" || $ref =~ m/JSON::PP::Boolean/ || !$ref ) {
            push( @out, $val );
        }
        elsif ( $ref eq "ARRAY" ) {
            push( @out, [ $self->_translate_raw_in( $depth + 1, @$val ) ] );
        }
        elsif ( $ref eq "HASH" ) {
            my $tmph = {};
            foreach my $k ( keys(%$val) ) {
                my ($tmpout) = $self->_translate_raw_in( $depth + 1, $val->{$k} );
                $tmph->{$k} = $tmpout;
            }
            push( @out, $tmph );
        }
        else {
            die "Unable to parse ($ref($val))\n";
        }
    }

    return @out;
}

# Begin-Doc
# Name: _translate_raw_out
# Description: strip out the wrapper markers and quotes
# Usage: my $txt = $obj->_translate_raw_out($txt)
# End-Doc

sub _translate_raw_out {
    my $self    = shift;
    my $content = shift;
    my $wrapper = $Local::JSON::wrapper;

    my $txt = $content;
    $txt =~ s/"${wrapper}(.*?)${wrapper}"/decode_base64($1)/sgme;
    return $txt;
}

#
# Helper
#
package Local::JSON::Raw;

# Begin-Doc
# Name: new
# Description: new Local::JSON::Raw();
# End-Doc

sub new {
    my $self   = shift;
    my $class  = ref($self) || $self;
    my $scalar = shift;

    my $tmp = {};
    $tmp->{value} = $scalar;

    $Local::JSON::used_raw++;
    return bless $tmp, $class;
}

1;

