
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

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw(
  Encode_HashToFile
  Encode_FileToHash
  Encode_HashToFileH
  Encode_FileHToHash
  Encode_URLEncode
  Encode_URLDecode
  Encode_HashToString
  Encode_StringToHash
  Encode_HTMLEncode
  Encode_HTMLDecode
);

# Begin-Doc
# Name: Encode_HashToFile
# Type: function
# Description: Writes an associative array to a file in an encoded format
# Syntax: &Encode_HashToFile($filename, %hash);
# End-Doc
sub Encode_HashToFile {
    my ( $file, %hash ) = @_;
    my ( $key, $val );

    open( ENCODEFILE, ">$file" );
    while ( ( $key, $val ) = each %hash ) {
        $key = &Encode_URLEncode($key);
        $val = &Encode_URLEncode($val);

        print ENCODEFILE "$key=$val\n";
    }
    close(ENCODEFILE);
}

# Begin-Doc
# Name: Encode_FileToHash
# Type: function
# Description: Decodes a hash from a file and returns it
# Syntax: %hash = &Encode_FileToHash($filename);
# End-Doc
sub Encode_FileToHash {
    my ($file) = @_;
    my (%hash);
    my ( $line, $key, $val );

    open( ENCODEFILE, "$file" );
    while ( $_ = <ENCODEFILE> ) {
        chop $_;
        ( $key, $val ) = split( "=", $_ );

        $key = &Encode_URLDecode($key);
        $val = &Encode_URLDecode($val);

        $hash{$key} = $val;
    }
    close(ENCODEFILE);

    return %hash;
}

# Begin-Doc
# Name: Encode_HashToFileH
# Type: function
# Description: Writes a hash to a FileHandle in encoded format
# Syntax: &Encode_HashToFileH($fh, %hash);
# End-Doc
sub Encode_HashToFileH {
    my ( $fileh, %hash ) = @_;
    my ( $key, $val );

    while ( ( $key, $val ) = each %hash ) {
        $key = &Encode_URLEncode($key);
        $val = &Encode_URLEncode($val);

        print $fileh "$key=$val\n";
    }
}

# Begin-Doc
# Name: Encode_FileHToHash
# Type: function
# Description: Reads a encoded hash from a FileHandle and returns it
# Syntax: %hash = &Encode_FileHToHash($fh);
# End-Doc
sub Encode_FileHToHash {
    my ($fileh) = @_;
    my (%hash);
    my ( $line, $key, $val );

    while ( $_ = <$fileh> ) {
        chop $_;
        ( $key, $val ) = split( "=", $_ );

        $key = &Encode_URLDecode($key);
        $val = &Encode_URLDecode($val);

        $hash{$key} = $val;
    }

    return %hash;
}

# Begin-Doc
# Name: Encode_URLEncode
# Type: function
# Description: Encodes a string in URL encoded format
# Syntax: $string = &Encode_URLEncode($string)
# Comments: All chars other than [A-Za-z0-9-_] are converted to %XX hex notation
# End-Doc
sub Encode_URLEncode {
    my ($string) = @_;
    my ( @tmp, @res, $tmp );

    @res = ();
    @tmp = split( '', $string );
    foreach $tmp (@tmp) {
        if ( $tmp =~ /[A-Za-z0-9-_]/ ) {
            push( @res, $tmp );
        }
        else {
            $tmp = unpack( "C", $tmp );
            $tmp = "%" . sprintf( "%.2X", $tmp );
            push( @res, $tmp );
        }
    }

    $string = join( "", @res );
    return $string;
}

# Begin-Doc
# Name: Encode_URLDecode
# Type: function
# Description: Decodes a url-encoded string
# Syntax: $string = &Encode_URLDecode($string);
# End-Doc
sub Encode_URLDecode {
    my ($string) = @_;

    $string =~ tr/+/ /;
    $string =~ s/%(..)/pack("C",hex($1))/ge;

    return $string;
}

# Begin-Doc
# Name: Encode_HashToString
# Type: function
# Description: url-encodes an entire hash into a single string
# Syntax: $string = &Encode_HashToString(%hash)
# Comments: the resulting string is essentially equivalent to the args to a cgi request
# End-Doc
sub Encode_HashToString {
    my (%hash) = @_;
    my ( $key, $val );
    my (@tmp);

    @tmp = ();
    while ( ( $key, $val ) = each %hash ) {
        $key = &Encode_URLEncode($key);
        $val = &Encode_URLEncode($val);
        push( @tmp, "$key=$val" );
    }

    return join( "&", @tmp );
}

# Begin-Doc
# Name: Encode_StringToHash
# Type: function
# Description: Decodes a url-encoded string into a hash
# Syntax: %hash = &Encode_StringToHash($string)
# Comments: very similar to processing the POST'd or GET data for a cgi request
# End-Doc
sub Encode_StringToHash {
    my ($string) = @_;
    my ( %hash, @tmp, $key, $val, $pair );

    @tmp = split( "&", $string );
    foreach $pair (@tmp) {
        ( $key, $val ) = split( "=", $pair );

        $key = &Encode_URLDecode($key);
        $val = &Encode_URLDecode($val);

        $hash{$key} = $val;
    }
    close(ENCODEFILE);

    return %hash;
}

# Begin-Doc
# Name: Encode_HTMLEncode
# Type: function
# Description: HTML encodes a string
# Syntax: $string = &Encode_HTMLEncode($text)
# Comments: probably not complete
# End-Doc
sub Encode_HTMLEncode {
    my ($string) = @_;

    $string =~ s/&/&amp;/gio;
    $string =~ s/</&lt;/gio;
    $string =~ s/>/&gt;/gio;

    return $string;
}

# Begin-Doc
# Name: Encode_HTMLDecode
# Type: function
# Description: HTML decodes a string
# Syntax: $string = &Encode_HTMLDecode($text)
# Comments: probably not complete
# End-Doc
sub Encode_HTMLDecode {
    my ($string) = @_;

    $string =~ s/&gt;/>/gio;
    $string =~ s/&lt;/</gio;
    $string =~ s/&amp;/&/gio;

    return $string;
}

1;

