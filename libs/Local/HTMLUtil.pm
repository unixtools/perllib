#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#

=begin

Begin-Doc
Name: Local::HTMLUtil
Type: module
Description: HTML/CGI utilities
End-Doc

=cut

package Local::HTMLUtil;
require Exporter;
use strict;
use Local::UsageLogger;

# We are now a wrapper around CGI for encoding/decoding routines
use CGI qw/-private_tempfiles/;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw(
    HTMLGetCGI

    HTMLScriptURL
    HTMLInputFile
    HTMLGetFile
    HTMLInputText
    HTMLInputPassword
    HTMLTextArea
    HTMLLink
    HTMLRadioButton
    HTMLHidden
    HTMLCheckbox
    HTMLSubmit
    HTMLReset
    HTMLStartMultiForm
    HTMLStartForm
    HTMLEndForm
    HTMLStartSelect
    HTMLSelectAssocArray
    HTMLSelectArray
    HTMLEndSelect
    HTMLSelectItem
    HTMLButton
    HTMLRule
    HTMLPre
    HTMLEndPre
    HTMLTitle
    HTMLAddress
    HTMLFooter
    HTMLStatusHeader
    HTMLContentType
    HTMLSentContentType
    HTMLGetRequest

    HTMLGetCookies
    HTMLSetCookies

    %rqpairs
);

BEGIN {

    # since we may not be running under cgiwrap, for easier error management
    if ( $ENV{REQUEST_METHOD} ) {
        open( STDERR, ">&STDOUT" );
    }
}

my $CGI;

# Begin-Doc
# Name: HTMLUtil_Sent_CType
# Description: package var to keep track of whether content type was sent
# End-Doc
my $HTMLUtil_Sent_CType = 0;

# Begin-Doc
# Name: HTMLGetCGI
# Type: function
# Syntax: $cgi = &HTMLGetCGI();
# Description: Returns reference to the CGI module object that HTMLUtil uses internally
# Comments: Must be called AFTER &HTMLGetRequest.
# End-Doc
sub HTMLGetCGI {
    return $CGI;
}

# Begin-Doc
# Name: HTMLScriptURL
# Type: function
# Syntax: $string = &HTMLScriptURL;
# Description: Returns URL of the currently executing script
#
# Comment: Returns the URL of the currently executing script, calculated
# from SERVER_NAME, SERVER_PORT, and other environment variables. Use this
# instead of hardcoding a URL to your script to allow the script to easily
# be relocated.
#
# End-Doc
sub HTMLScriptURL {
    my ( $port, $prefix, $hostport, $scriptname, $url );

    if ( $ENV{"HTTPS"} ne "on" ) {
        $prefix = "http";
    }
    else {
        $prefix = "https";
    }
    $scriptname = $ENV{"SCRIPT_NAME"};
    $hostport   = $ENV{"HTTP_HOST"};

    if ( $hostport eq "" ) {
        $hostport = $ENV{"SERVER_NAME"} . ":" . $ENV{"SERVER_PORT"};
    }

    # Tack on a port if it wasn't already present
    if (   $ENV{SERVER_PORT} ne 80
        && $ENV{SERVER_PORT} ne 443
        && $hostport !~ /:/ )
    {
        $hostport .= ":" . $ENV{SERVER_PORT};
    }

    $url = "$prefix://$hostport$scriptname";
    return $url;
}

# Begin-Doc
# Name: HTMLInputText
# Type: function
# Description: print out a text input box
# Syntax: &HTMLInputText($name,$size,$default,$maxlength)
# Comments: Prints out an input text field, with name $name. If size is
# not empty, it prints out a size=$size parameter. If defaults is not
# empty, it sets the default value of the input box to be $default. If
# $maxlength is non-zero, it sets the maximum input length to be $maxlength.
# End-Doc
sub HTMLInputText {
    my ( $NAME, $SIZE, $VALUE, $MAXLENGTH ) = @_;

    print "<input type=\"text\" name=\"$NAME\"";

    if ( $VALUE ne "" ) {
        print " value=\"$VALUE\"";
    }

    if ( $SIZE ne "" ) {
        print " size=\"$SIZE\"";
    }

    if ( int($MAXLENGTH) > 0 ) {
        print " maxlength=\"$MAXLENGTH\"";
    }

    print '>';
}

# Begin-Doc
# Name: HTMLInputFile
# Type: function
# Description: file selection field, requires a multipart form
# Syntax: &HTMLInputFile($name,$size,$default,$maxlength)
# Comments: Prints out an file upload field, with name $name. If size is
# not empty, it prints out a size=$size parameter. If defaults is not
# empty, it sets the default value of the input box to be $default. Note - most
# browsers ignore the default value. If $maxlength is non-zero, it sets the maximum
# input length to be $maxlength.
# End-Doc
sub HTMLInputFile {
    my ( $NAME, $SIZE, $VALUE, $MAXLENGTH ) = @_;

    print "<input type=\"file\" name=\"$NAME\"";

    if ( $VALUE ne "" ) {
        print " value=\"$VALUE\"";
    }

    if ( $SIZE ne "" ) {
        print " size=\"$SIZE\"";
    }

    if ( int($MAXLENGTH) > 0 ) {
        print " maxlength=\"$MAXLENGTH\"";
    }

    print '>';
}

# Begin-Doc
# Name: HTMLInputPassword
# Type: function
# Description: same as HTMLInputText, but with a password type field
# Syntax: &HTMLInputPassword($name,$size,$default,$maxlength)
# End-Doc
sub HTMLInputPassword {
    my ( $NAME, $SIZE, $VALUE, $MAXLENGTH ) = @_;

    print '<input type="password" name="', $NAME, '"';

    if ( $VALUE ne "" ) {
        print ' value="', $VALUE, '"';
    }

    if ( $SIZE ne "" ) {
        print ' size=', $SIZE;
    }

    if ( int($MAXLENGTH) > 0 ) {
        print ' maxlength="', int($MAXLENGTH), '"';
    }

    print '>';
}

# Begin-Doc
# Name: HTMLTextArea
# Type: function
# Description: print out a text area input box
# Syntax: &HTMLTextArea($name,$value,$width,$heigh,$wrap);
# Comments: Prints out a text area, pre-filled in with text "$value",
#        $width columns wide, and $height rows in length. If $wrap is
#        set, will add a WRAP= header. If $wrap is BOTH, will do
#        WRAP=HARD WRAP=PHYSICAL.  If $wrap is set to PHYSICAL, text is wrapped
#        and the result is sent to the web server with the line breaks included.
#        If $wrap is set to VIRTUAL, text is wrapped within the textarea, but
#        the result is sent to the web server without the wrapping information.
# End-Doc
sub HTMLTextArea {
    my ( $NAME, $VALUE, $WIDTH, $HEIGHT, $WRAP ) = @_;

    if ( $WIDTH == 0 )  { $WIDTH  = 45; }
    if ( $HEIGHT == 0 ) { $HEIGHT = 10; }

    print "\n<textarea name=\"$NAME\" rows=$HEIGHT cols=$WIDTH ";
    if ( uc $WRAP eq "BOTH" ) {
        print "wrap=hard wrap=physical ";
    }
    elsif ( $WRAP ne "" ) {
        print "wrap=\"$WRAP\" ";
    }
    print " >";
    print $VALUE;
    print "</textarea>";
}

# Begin-Doc
# Name: HTMLLink
# Type: function
# Description: prints out a link to another doc with label
# Syntax: &HTMLLink($url,$label)
# Comments: Prints out a &lt;A HREF=$url&gt;$name&lt;/A&gt; link to another url.
# End-Doc
sub HTMLLink {
    my ( $URL, $NAME ) = @_;
    print "<a href=\"$URL\">$NAME</A>";
}

# Begin-Doc
# Name: HTMLRadioButton
# Type: function
# Description: prints ouf a input radio button
# Syntax: &HTMLRadioButton($name,$value,$on,$text);
# Comments: Prints out a input radio button, with name $name. Sets
# returned value to $value. If $on is 1, the readio button is pre-checked.
# If $text is non-empty, prints $text after the radio button.
# End-Doc
sub HTMLRadioButton {
    my ( $NAME, $VALUE, $ON, $TEXT ) = @_;

    print '<input type="radio" name="', $NAME, '" value="', $VALUE, '" ';
    print " checked " if $ON;
    print '>';

    if ( $TEXT ne "" ) {
        print $TEXT;
    }

}

# Begin-Doc
# Name: HTMLHidden
# Type: function
# Description: prints out a hidden form var
# Syntax: &HTMLHidden($name,$value);
# Comments: Prints out a hidden type field with name $name and value $value.
# End-Doc
sub HTMLHidden {
    my ( $NAME, $VALUE ) = @_;
    print "<input type=\"hidden\" name=\"$NAME\" value=\"$VALUE\">";
}

# Begin-Doc
# Name: HTMLCheckbox
# Type: function
# Description: print out a checkbox
# Syntax: &HTMLCheckbox($name,$on);
# Comments: Prints out a checkbox, with name $name, prechecked if $on is 1.
# End-Doc
sub HTMLCheckbox {
    my ( $NAME, $ON ) = @_;

    print '<input type="checkbox" name="', $NAME, '"';

    print " checked " if $ON;

    print '>';
}

# Begin-Doc
# Name: HTMLSubmit
# Type: function
# Description: Print out a submit button
# Syntax: &HTMLSubmit($label, $name);
# End-Doc
sub HTMLSubmit {
    my ( $LABEL, $NAME ) = @_;

    if ( $NAME ne "" ) {
        print "<input type=\"submit\" value=\"$LABEL\" name=\"$NAME\">";
    }
    else {
        print "<input type=\"submit\" value=\"$LABEL\">";
    }
}

# Begin-Doc
# Name: HTMLReset
# Type: function
# Description: Prints out a reset button
# Syntax: &HTMLReset();
# End-Doc
sub HTMLReset {
    my ($LABEL) = @_;
    if ($LABEL) {
        print "<input type=\"reset\" value=\"$LABEL\">";
    }
    else {
        print "<input type=\"reset\">";
    }
}

# Begin-Doc
# Name: HTMLStartForm
# Type: function
# Description: Prints out an opening form tag
# Syntax: &HTMLStartForm($action,[$method,[$name]]);
# Comments: Prints out an opening form tag, method of POST (or $method), with action $action, and optional form name $name
# being $url.
# End-Doc
sub HTMLStartForm {
    my ( $ACTION, $METHOD, $NAME ) = @_;

    if ( $ACTION eq "" ) { $ACTION = &HTMLScriptURL(); }

    if ( $METHOD eq "" ) {
        $METHOD = "POST";
    }

    print "<form method=$METHOD action=\"$ACTION\" ";
    if ( $NAME ne "" ) {
        print " name=\"$NAME\" ";
    }
    print ">";
}

# Begin-Doc
# Name: HTMLStartMultiForm
# Type: function
# Description: Prints out an open form tag for a multipart form
# Syntax: &HTMLStartForm($action, [$name]);
# Comments: Prints out an opening form tag, method must be POST, with action $action, and optional form name $name
# End-Doc
sub HTMLStartMultiForm {
    my ( $ACTION, $NAME ) = @_;

    if ( $ACTION eq "" ) { $ACTION = &HTMLScriptURL(); }

    print "<form method=POST enctype=\"multipart/form-data\" action=\"$ACTION\" ";
    if ( $NAME ne "" ) {
        print " name=\"$NAME\" ";
    }
    print ">";
}

# Begin-Doc
# Name: HTMLEndForm
# Type: function
# Description: end the form
# Syntax: &HTMLEndForm();
# End-Doc
sub HTMLEndForm {
    print "</form>";
}

# Begin-Doc
# Name: HTMLStartSelect
# Type: function
# Description: starts a select box
# Syntax: &HTMLStartSelect($name,$size,$multiple);
# Comments: Starts a select box, with name $name, and selection size
# $size. If size is 1, will be a popup, otherwise will be a list box. If
# $multiple is present and non-zero, select list will support multiple selections.
# End-Doc
sub HTMLStartSelect {
    my ( $NAME, $SIZE, $MULTIPLE ) = @_;
    my $multopt = "";

    if ($MULTIPLE) {
        $multopt = "multiple";
    }

    if ( $SIZE ne "" ) {
        print "<select name=\"$NAME\" size=\"$SIZE\" $multopt>";
    }
    else {
        print "<select name=\"$NAME\" $multopt>";
    }
}

# Begin-Doc
# Name: HTMLSelectAssocArray
# Type: function
# Description: build a select box from a hash
# Syntax: &HTMLSelectAssocArray($name,$size,$blank,$select,%data);
# Comments: Prints out an select box, size $size, name $name, for each
# key/value pair in associative array %data. If $blank is non-zero, a blank
# entry will be added to the select box to allow an empty choice. If
# $select is non-empty the key/value with key=$select will be preselected
# and place at the top of the select box.
# End-Doc
sub HTMLSelectAssocArray {
    my ( $NAME, $SIZE, $BLANK, $select, %DATA ) = @_;
    my (@TEMP);
    my (%TEMPASSOC);

    @TEMP = %DATA;

    if ( $#TEMP > 0 ) {
        &HTMLStartSelect( $NAME, int($SIZE) );

        if ( $select ne "" ) {
            if ( $DATA{$select} eq "" ) {
                $DATA{$select} = "Unknown ($select)";
            }
            &HTMLSelectItem( $select, $DATA{$select}, 1 );
        }

        if ($BLANK) {
            &HTMLSelectItem( "", "Blank" );
        }

        #
        # This is a retchedly inneficient way to print the list sorted, but
        # was quick to implement, should improve efficiency at some point.
        #
        foreach my $KEY ( keys(%DATA) ) {
            $TEMPASSOC{ $DATA{$KEY} } = $KEY;
        }

        foreach my $VALUE ( sort( keys(%TEMPASSOC) ) ) {
            if ( $select eq $TEMPASSOC{$VALUE} ) {

                #			&HTMLSelectItem($TEMPASSOC{$VALUE}, $VALUE, 1);
            }
            else {
                &HTMLSelectItem( $TEMPASSOC{$VALUE}, $VALUE );
            }
        }

        &HTMLEndSelect;
    }
}

# Begin-Doc
# Name: HTMLSelectArray
# Type: function
# Syntax: &HTMLSelectArray($varname, $size, $selection, @array)
# Description: create a select box based on array
# Comments: $varname is the name of the variable in quotes.
#         if $size equals one, the select box will be a popup, otherwise it will be a listbox.
#         $selection will be the default value
#         @array is a list of the select box
# Comments: does not encoded any of the values, so make sure they are HTML-safe
# End-Doc
sub HTMLSelectArray {
    my ( $varname, $size, $selection, @array ) = @_;

    &HTMLStartSelect( $varname, $size );

    foreach my $sel (@array) {
        if ( $selection eq $sel ) {
            &HTMLSelectItem( $sel, $sel, 1 );
        }
        else {
            &HTMLSelectItem( $sel, $sel, 0 );
        }
    }
    &HTMLEndSelect();
}

# Begin-Doc
# Name: HTMLEndSelect
# Type: function
# Description: ends a select box
# Syntax: &HTMLEndSelect();
# End-Doc
sub HTMLEndSelect {
    print "</select>";
}

# Begin-Doc
# Name: HTMLSelectItem
# Type: function
# Description: prints out a item for a select box
# Syntax: &HTMLSelectItem($value,$name,$selected);
# Comments: Prints out an option tag, pre-selected if $selected is 1,
# with a special value= if $value is non-empty, and the contents will be $name.
# End-Doc
sub HTMLSelectItem {
    my ( $VALUE, $NAME, $selected ) = @_;

    print "<option value=\"$VALUE\" ";

    if ($selected) {
        print " selected ";
    }

    print ">$NAME\n";
}

# Begin-Doc
# Name: HTMLButton
# Type: function
# Description: prints out self-contained form button that opens a url
# Syntax: &HTMLRule($url,$linklabel);
# End-Doc
sub HTMLButton {
    my ( $URL, $NAME ) = @_;

    &HTMLStartForm($URL);
    &HTMLSubmit($NAME);
    &HTMLEndForm;
}

# Begin-Doc
# Name: HTMLRule
# Type: function
# Description: prints out a horiz-rule tag
# Syntax: &HTMLRule;
# End-Doc
sub HTMLRule {
    print "<hr>";
}

# Begin-Doc
# Name: HTMLPre
# Type: function
# Description: starts a preformatted block
# Syntax: &HTMLEndPre;
# End-Doc
sub HTMLPre {
    print "<pre>";
}

# Begin-Doc
# Name: HTMLEndPre
# Type: function
# Description: ends the preformatted block
# Syntax: &HTMLEndPre;
# End-Doc
sub HTMLEndPre {
    print "</pre>";
}

# Begin-Doc
# Name: HTMLTitle
# Type: function
# Description: sets document title and prints title centered as header
# Syntax: &HTMLTitle($text);
# Comments: Prints out a TITLE tag containing $text, also prints out a
# centered header containing $text.
# End-Doc
sub HTMLTitle {
    my ($TEXT) = @_;

    print "<title>$TEXT</title>\n";
    print "<center><h1>$TEXT</h1></center>\n";
}

# Begin-Doc
# Name: HTMLAddress
# Type: function
# Description: prints out a mailto link
# Syntax: &HTMLAddress($addr);
# Comments: Prints out an address inside ADDRESS tags, with the contents
# being a mailto: link.
# End-Doc
sub HTMLAddress {
    my ($ADDR) = @_;

    print "<address>";
    &HTMLLink( "mailto:$ADDR", $ADDR );
    print "</address>";

}

# Begin-Doc
# Name: HTMLFooter
# Description: prints out a standardized footer
# Syntax: &HTMLFooter($servername,$serverurl,$maintaineremail)
# Comments: Prints out a simple footer with a link to the $serverurl,
# labelled $servername, and a mailto link to $maintaineremail.
# End-Doc
sub HTMLFooter {
    my ( $SERV, $SERVURL, $ADDR ) = @_;

    print "This document is located on the ";
    &HTMLLink( $SERVURL, $SERV );
    print ". Comments/questions/etc. to ";
    &HTMLAddress($ADDR);
}

# Begin-Doc
# Name: HTMLStatusHeader
# Type: function
# Description: outputs a http status header for nph scripts
# Syntax: &HTMLStatusHeader()
# End-Doc
sub HTMLStatusHeader {
    print "HTTP/1.0 200 Ok\n";
}

# Begin-Doc
# Name: HTMLContentType
# Type: function
# Description: prints ouf a content-type header and double-newline
# Syntax: &HTMLContentType($type);
# Comments: $type defaults to "text/html"
# End-Doc
sub HTMLContentType {
    my ($TYPE) = @_;

    if ( $TYPE eq "" ) { $TYPE = "text/html"; }

    print "Content-type: $TYPE\n\n";

    $HTMLUtil_Sent_CType = 1;
}

# Begin-Doc
# Name: HTMLSentContentType
# Type: function
# Description: returns if the HTMLContentType routine has been used, also can be used to set that state
# Syntax: $was_sent = &HTMLSentContentType()
# Syntax: &HTMLSentContentType(0|1);
# End-Doc
sub HTMLSentContentType {
    my $val = shift;

    if ( defined($val) ) {
        $HTMLUtil_Sent_CType = ( $val != 0 );
    }
    return $HTMLUtil_Sent_CType;
}

# Begin-Doc
# Name: HTMLGetFile
# Type: function
# Description: returns filehandle for a file uploaded by a multipart form
# Syntax: $fh = &HTMLGetFile("fieldname");
# Comments: returns undef if no file uploaded
# End-Doc
sub HTMLGetFile {
    my ($name) = @_;

    return $CGI->upload($name);
}

# Begin-Doc
# Name: HTMLGetRequest
# Type: function
# Description: retrieves cgi request parameters into %rqpairs and %in
# Syntax: &HTMLGetRequest();
# Comments: Decodes the request from the server into the associative
# array %rqpairs. Entries that result from multiple selections in a list
# box will be stored into a single element, with the values separated by
# whitespace.
# End-Doc
sub HTMLGetRequest {
    my ( $request, $key, $val, $tmp, %tmp, $entry );

    $CGI = new CGI();

    %main::rqpairs = ();
    foreach my $key ( $CGI->param ) {
        $main::rqpairs{$key} = join( " ", $CGI->param($key) );
    }
    *main::in = *main::rqpairs;

    # If we're running under mod_perl, to keep semantics, export into callers namespace as well
    my ($pkg) = caller(0);
    if ( $pkg =~ /^ModPerl/ ) {
        no strict "refs";
        *{ $pkg . "::rqpairs" } = *main::rqpairs;
    }
}

# Begin-Doc
# Name: HTMLGetCookies
# Type: function
# Description: retrieves cookies from web request, returns hash
# Syntax: %cookies = &HTMLGetCookies();
# End-Doc
sub HTMLGetCookies {
    my ( $key, $val, %results );
    my $cookie_string = $ENV{HTTP_COOKIE} || $ENV{COOKIE};
    return () unless $cookie_string;
    my (@pairs) = split( "; ", $cookie_string );

    foreach (@pairs) {
        if (/^([^=]+)=(.*)/) {
            $key = $1;
            $val = $2;
        }
        else {
            $key = $_;
            $val = '';
        }
        $results{$key} = $val;
    }
    return %results;
}

# Begin-Doc
# Name: HTMLSetCookies
# Type: function
# Description: sets cookies in web response
# Syntax: &HTMLSetCookies(@cookies);
# Comments: Should be called prior to HTMLContentType
# Comments: Array contains hash refs with keys name, value, domain, path, expires
# End-Doc
sub HTMLSetCookies {
    my @cookies = @_;
    my $time    = time;
    my ( $cookie, $exp, $offset, $unit, $change, @date, $datestr, $cookie_string );

    my @days = ( 'Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat' );
    my @months = ( 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec' );

    foreach $cookie (@cookies) {
        my %data = %{$cookie};

        $datestr = undef;
        $exp     = $data{expires};
        $exp =~ s/\s//g;
        if ( $exp =~ /^(\+?)([0-9]+)([smhdy]?)$/i ) {
            $offset = $2;
            $unit   = $3;

            if ( $unit eq "s" or $unit eq "" ) { $change = $offset }
            elsif ( $unit eq "m" ) { $change = 60 * $offset }
            elsif ( $unit eq "h" ) { $change = 3600 * $offset }
            elsif ( $unit eq "d" ) { $change = 86400 * $offset }
            elsif ( $unit eq "y" ) { $change = 31536000 * $offset }
            else                   { $change = 0 }
            @date = gmtime( $time + $change );

            # this seems to work, MSIE doesn't follow the rfc for cookies DD-MON-YYYY date format
            $datestr = sprintf(
                "%s, %.2d %s %.4d %.2d:%.2d:%.2d GMT",
                $days[ $date[6] ],
                $date[3],
                $months[ $date[4] ],
                $date[5] + 1900,
                $date[2], $date[1], $date[0]
            );

        }
        $cookie_string .= "Set-Cookie: " . $data{name} . "=" . $data{value};
        if ( $data{domain} ) {
            $cookie_string .= "; domain=" . $data{domain};
        }
        if ( $data{path} ) { $cookie_string .= "; path=" . $data{path}; }
        if ($datestr)      { $cookie_string .= "; expires=" . $datestr; }
        $cookie_string .= "\n";
    }
    @date = gmtime($time);
    $datestr
        = "Date: "
        . $days[ $date[6] ]
        . ", $date[3] $months[$date[4]] "
        . ( $date[5] + 1900 )
        . " $date[2]:$date[1]:$date[0] GMT";
    $cookie_string .= $datestr . "\n";

    print $cookie_string;
}

1;

