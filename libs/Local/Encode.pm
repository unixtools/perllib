#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: http://svn.unixtools.org/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#

=begin
Begin-Doc
Name: Local::Encode
Type: module
Description: data marshalling/encoding/serialization routines
End-Doc
=cut

package Local::Encode;
require 5.000;
require Exporter;
use strict;
use URI::Escape;
use HTML::Entities;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw(
    Encode_URLEncode
    Encode_URLDecode
    Encode_HTMLEncode
    Encode_HTMLDecode
);

# Begin-Doc
# Name: Encode_URLEncode
# Type: function
# Description: Encodes a string in URL encoded format
# Syntax: $string = &Encode_URLEncode($string)
# Comments: All chars other than [A-Za-z0-9-_] are converted to %XX hex notation
# End-Doc
sub Encode_URLEncode {
    my $string = shift;

    return uri_escape($string);
}

# Begin-Doc
# Name: Encode_URLDecode
# Type: function
# Description: Decodes a url-encoded string
# Syntax: $string = &Encode_URLDecode($string);
# End-Doc
sub Encode_URLDecode {
    my $string = shift;

    return uri_unescape($string);
}

# Begin-Doc
# Name: Encode_HTMLEncode
# Type: function
# Description: HTML encodes a string
# Syntax: $string = &Encode_HTMLEncode($text)
# Comments: probably not complete
# End-Doc
sub Encode_HTMLEncode {
    my $string = shift;

    return encode_entities($string);
}

# Begin-Doc
# Name: Encode_HTMLDecode
# Type: function
# Description: HTML decodes a string
# Syntax: $string = &Encode_HTMLDecode($text)
# Comments: probably not complete
# End-Doc
sub Encode_HTMLDecode {
    my $string = shift;

    return decode_entities($string);
}

1;

