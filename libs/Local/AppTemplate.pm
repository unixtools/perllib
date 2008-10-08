
=pre

Begin-Doc
Name: Local::AppTemplate
Type: module
Description: Standardized UMR IT Application HTML Template Routines
Comments: This module collects together the routines necessary to use the standard application
Comments: template and some standard IT web widgets.

Example:

	use Local::AppTemplate;

	$html = new Local::AppTemplate(title => "My App",
		[other parms if you want to set them]);

	$html->PageHeader();

	if (error) { $html->ErrorExit($msg_to_be_output_and_htmlencoded); }

	$db->SQL_ExecQuery($qry) || $html->ErrorExitSQL("My msg", $db);

	$html->PageFooter();

End-Doc

=cut

package Local::AppTemplate;
require 5.000;
require Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use LWP::Simple;
use File::Path;

@ISA    = qw();
@EXPORT = qw();

# Begin-Doc
# Name: new
# Type: function
# Description: Creates object
# Syntax: $html = new Local::AppTemplate(%params)
# Example: The following replacable parameters exist
#
#  title - page title (always recommended)
#  apptitle - application subtitle
#  stylesheet - app override stylesheet placed after global stylesheets
#  style - inline style placed after app stylesheet inclusion
#  head_extra - inline raw content placed after app stylesheet inclusion
#  contact_url - contact url target
#  contact_label - contact url label
#  app_url - defaults to &HTMLScriptURL(), can override if you have a main app page to link to
#  quiet - do not show details of error messages or stack traces/etc., defaults to showing for now
#  refresh_time - meta refresh the page after this many seconds
#  refresh_url - instead of refreshing to same page, refresh to this URL
#  template_path - scalar or array of scalars pointing at locations to try accessing the template
#  template_cache_dir - override default location where remote templates are cached
#
#
# End-Doc
sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;
    my $tmp   = {};

    my $config = {};
    $tmp->{config} = $config;

    # parameters loaded from initialization
    $config->{title}         = $opts{title} || "Web Application";
    $config->{apptitle}      = $opts{apptitle};
    $config->{headerimage}   = $opts{headerimage};
    $config->{stylesheet}    = $opts{stylesheet};
    $config->{style}         = $opts{style};
    $config->{contact_url}   = $opts{contact_url};
    $config->{app_url}       = $opts{app_url};
    $config->{head_extra}    = $opts{head_extra};
    $config->{contact_label} = $opts{contact_label};

    $config->{refresh_time} = $opts{refresh_time};
    $config->{refresh_url}  = $opts{refresh_url};

    my $at_env_path = $ENV{APPTEMPLATE_PATH};

    if ( defined( $opts{template_path} ) ) {
        if ( !ref( $opts{template_path} ) ) {
            $config->{template_path} = [ $opts{template_path} ];
        }
        elsif ( defined( $opts{template_path} ) ) {
            $config->{template_path} = $opts{template_path};
        }
    }
    elsif ( $at_env_path =~ m|^https*://.*| ) {
        # Only permit setting a remote template path via URL
        $config->{template_path} = [$at_env_path];
    }
    elsif ( !$config->{template_path} ) {
        $config->{template_path} = ["/local/apptmpl/html"];
    }

    $config->{template_cache_dir} = $opts{template_cache_dir};

    if ( $opts{quiet} ) {
        $config->{quiet} = $opts{quiet};
    }
    else {
        $config->{quiet} = 0;
    }

    # internal tracking data
    $tmp->{open_blocks}      = [];
    $tmp->{inner_row_number} = 0;

    return bless $tmp, $class;
}

#
# Template retrieval
#

# Begin-Doc
# Name: _load_template
# Type: method
# Description: outputs a page header
# Syntax: $obj->PageHeader();
# Comments: If you have specified a header image, it will use that image file for the header
# Comments: instead of a textual title area.
# End-Doc
sub _load_template {
    my $self   = shift;
    my $config = $self->{config};

    # Prevent re-running the template
    return if ( $self->{template_loaded} );

    my $text;
    foreach my $location ( @{ $config->{template_path} } ) {
        if ( -f $location ) {
            open( TEMPLATE_IN, "<", $location );
            $text = join( "", <TEMPLATE_IN> );
            close(TEMPLATE_IN);
        }
        elsif ( -f "$location/index.html" ) {
            open( TEMPLATE_IN, "<", $location . "/index.html" );
            $text = join( "", <TEMPLATE_IN> );
            close(TEMPLATE_IN);
        }
        elsif ( $location =~ m{^(http|https|ftp|file)://} ) {
            my $cache = $config->{template_cache_dir};

            #
            # Try to calculate a default if needed
            #
            if ( !$cache ) {
                my $home;
                eval { $home = ( getpwuid($>) )[7]; };
                if ($home) {
                    $cache = $home . "/tmp/apptmpl-cache";
                    mkpath( [$cache], 0, 0700 );
                }
                elsif ($^O =~ /Win32/
                    && -e $ENV{TEMP}
                    && $ENV{TEMP} =~ m{docum}io )
                {
                    $cache = $ENV{TEMP} . "/apptmpl-cache";
                    mkpath( [$cache], 0, 0700 );
                }

                if ( !-d $cache ) {
                    undef($cache);
                }
            }

     #
     # If we have a cache, mirror the document, otherwise just do a straight GET
     # Might want to consider defining a minimum caching period, so we do not
     # attempt to cache the template repeatedly. There is also a potential
     # locking issue here, so we might want to use a temporary file if the LWP
     # mirror method isn't implemented atomically internally. (Looks like
     # mirror does a unlink+rename which should be good enough.)
     #
            if ( defined($cache) ) {

      # use a simple 256 bit checksum for the cache file name
      # feed it some extra parameters to get a touch more randomness in the name
                my $cachefilename = $cache . "/"
                  . sprintf( "%.8X",
                    unpack( "%256C*", join( "-", $location, $<, $>, $cache ) )
                  );

# don't try remirroring if we've modified the inode of the cache file in the last 30 seconds
                my @tmpstat = stat($cachefilename);
                unless ( time - $tmpstat[10] < 30 || time - $tmpstat[9] < 30 ) {
                    my $res = mirror( $location, "$cachefilename" );
                }

                if ( -f $cachefilename ) {
                    open( TEMPLATE_IN, "<$cachefilename" );
                    $text = join( "", <TEMPLATE_IN> );
                    close(TEMPLATE_IN);
                }
            }
            else {
                $text = get($location);
            }
        }
    }

    if ( !$text ) {
        $self->{template_text_header} =
          "<!-- unable to load template header -->";
        $self->{template_text_footer} =
          "<!-- unable to load template footer -->";
    }
    else {
        my ( $header, $footer ) = split( /__APP_CONTENT__/, $text, 2 );
        $self->{template_text_header} = $header;
        $self->{template_text_footer} = $footer;
    }

    $self->{template_loaded} = 1;
    return;
}

#
# Routines to track nesting of blocks
#
sub _push_block {
    my $self  = shift;
    my $block = shift;

    push( @{ $self->{open_blocks} }, $block );

    return;
}

sub _pop_block {
    my $self  = shift;
    my $block = shift;

    my $actual = $self->_peek_block();
    if ( $block ne $actual ) {
        print "<!-- pop block attempted for $block, got $actual -->";
        $self->ErrorExit(
"Invalid block nesting. Attempted closure of '$block', got '$actual'."
        );
    }

    pop( @{ $self->{open_blocks} } );

    return;
}

sub _peek_block {
    my $self = shift;

    my @blocks = @{ $self->{open_blocks} };
    return $blocks[$#blocks];
}

sub _server_base_url {
    my ( $prefix, $hostport, $url );

    if ( !$ENV{HTTP_HOST} ) {
        return "";
    }

    if ( $ENV{"HTTPS"} ne "on" ) {
        $prefix = "http";
    }
    else {
        $prefix = "https";
    }
    $hostport = $ENV{"HTTP_HOST"};

    if ( $hostport eq "" ) {
        $hostport = $ENV{"SERVER_NAME"} . ":" . $ENV{"SERVER_PORT"};
    }

    $url = "$prefix://$hostport";
    return $url;
}

#
# Filtering for replacement values
#
sub _filter {
    my $self = shift;
    my $text = shift;

    my $app_env = "prod";

    my $config = $self->{config};

    my $title = $config->{title};
    my $apptitle = $config->{apptitle} || $title;

    my $app_url   = $config->{app_url}       || "?";
    my $con_url   = $config->{contact_url}   || "/";
    my $con_label = $config->{contact_label} || "WebMaster";

    my $app_header_image = $config->{headerimage};

    my $app_head_pre;
    my $app_head_post;

    if ( $app_env ne "prod" ) {
        my $app_env_label =
          "<b><font color=\"\#BB1111\">" . uc($app_env) . "<\/font></b>";
        $title .= " - $app_env";
        $apptitle = $app_env_label . " - " . $apptitle . " - " . $app_env_label;
    }

    if (   $ENV{REMOTE_USER_IMPERSONATE}
        && $ENV{REMOTE_USER_IMPERSONATE} ne $ENV{REMOTE_USER_REAL} )
    {
        $title .= " [Impersonating: "
          . $self->Encode( $ENV{REMOTE_USER_IMPERSONATE} ) . "]";
    }

    if ( $config->{stylesheet} ) {
        $app_head_post .=
            "<link rel=\"stylesheet\" href=\""
          . $config->{stylesheet}
          . "\" type=\"text/css\" />";
    }
    if ( $config->{style} ) {
        $app_head_post .=
          "<style type=\"text/css\">" . $config->{style} . "</style>";
    }
    if ( $config->{head_extra} ) {
        $app_head_post .= $config->{head_extra};
    }

    if ( $config->{refresh_time} ) {
        my $time = int( $config->{refresh_time} );

        if ( $config->{refresh_url} ) {
            my $url = $config->{refresh_url};
            $app_head_pre .=
              "<meta http-equiv=\"Refresh\" CONTENT=\"${time};url=${url}\">";
        }
        else {
            $app_head_pre .=
              "<meta http-equiv=\"Refresh\" CONTENT=\"${time}\">";
        }
    }

    $text =~ s/__PAGE_TITLE__/$title/g;
    $text =~ s/__APP_URL__/$app_url/g;
    $text =~ s/__APP_TITLE__/$apptitle/g;
    $text =~ s/__APP_HEADER_IMAGE__/$app_header_image/g;
    $text =~ s/__APP_HEAD_PRE__/$app_head_pre/g;
    $text =~ s/__APP_HEAD_POST__/$app_head_post/g;
    $text =~ s/__CONTACT_LABEL__/$con_label/g;
    $text =~ s/__CONTACT_URL__/$con_url/g;

    my $remuser = $self->Encode( $ENV{REMOTE_USER} );
    $text =~ s/__REMOTE_USER__/$remuser/g;

    my $remhost = $self->Encode( $ENV{REMOTE_HOST} || $ENV{REMOTE_ADDR} );
    $text =~ s/__REMOTE_HOST__/$remhost/g;

    my $elaptime = ( time - $^T ) . " seconds";
    $text =~ s/__ELAPSED_TIME__/$elaptime/g;

    my $curtime = scalar( localtime(time) );
    $text =~ s/__CURRENT_TIME__/$curtime/g;

    my $base_url = $self->_server_base_url();
    $text =~ s/__BASE_URL__/$base_url/g;

    return $text;
}

# Begin-Doc
# Name: PageHeader
# Type: method
# Description: outputs a page header
# Syntax: $obj->PageHeader();
# Comments: If you have specified a header image, it will use that image file for the header
# Comments: instead of a textual title area.
# End-Doc
sub PageHeader {
    my $self   = shift;
    my $config = $self->{config};

    $self->_load_template();
    print $self->_filter( $self->{template_text_header} );

    $self->_push_block("Page");
}

# Begin-Doc
# Name: PageFooter
# Type: method
# Description: outputs the standard page footer
# Syntax: $obj->PageFooter();
# Comments: Will close all open blocks as well, but this should be considered bad form
# End-Doc
sub PageFooter {
    my $self   = shift;
    my $config = $self->{config};

    $self->_CloseNonPageBlocks();

    $self->_load_template();    # should be a no-op at this point
    print $self->_filter( $self->{template_text_footer} );

    # Need to address recursion?
    $self->_pop_block("Page");
}

# Begin-Doc
# Name: _CloseNonPageBlocks
# Type: method
# Description: closes all currently non-page nested blocks
# Syntax: $obj->_CloseNonPageBlocks();
# Access: internal, do not use outside of this module
# End-Doc
sub _CloseNonPageBlocks {
    my $self = shift;

    if ( $self->{closing_non_page_blocks} ) {
        print
          "<!-- CloseNonPageBlocks is non-recursive. Forcing termination. -->";
        return;
    }
    $self->{closing_non_page_blocks} = 1;

    my $cnt = 0;
    while ( my $block = $self->_peek_block() ) {
        $cnt++;
        last if ( $block eq "Page" );
        if ( $cnt > 100 ) {
            print "<!-- nesting too deep, forcing exit -->";
            last;
        }
        print "<!-- forcing close of $block -->";
        if ( $block eq "InnerRow" ) {
            $self->EndInnerRow();
        }
        if ( $block eq "InnerTable" ) {
            $self->EndInnerTable();
        }
        if ( $block eq "BlockTable" ) {
            $self->EndBlockTable();
        }
    }

    $self->{closing_non_page_blocks} = 0;
}

# Begin-Doc
# Name: RequirePriv
# Type: method
# Description: wrapper routine around privsys privilege check
# Syntax: $obj->RequirePriv($errmsg);
# End-Doc
sub RequirePriv {
    my $self = shift;
    my $code = shift;

    eval "use Local::PrivSys";

    if ( &PrivSys_CheckPriv( $ENV{REMOTE_USER}, $code ) ) {
        return;
    }
    else {
        $self->PrivErrorExit($code);
    }
}

# Begin-Doc
# Name: PrivErrorExit
# Type: method
# Description: prints a privilege required error msg in a block and terminates
# Syntax: $obj->PrivErrorExit($errmsg);
# End-Doc
sub PrivErrorExit {
    my $self = shift;
    my $code = shift;

    eval "use Local::PrivSys";

    $self->_CloseNonPageBlocks();

    if ( $self->{_in_error_exit} ) {
        print "<!-- error exit recursed, terminating app -->";
        exit;
    }

    $self->{_in_error_exit} = 1;
    $self->StartErrorBlockTable( "Insufficient Privileges", 600 );

    $self->StartInnerTable();

    $self->StartInnerRow();
    print "<td colspan=2 align=left>";
    print
"<p>You do not have the permissions required to access this application.\n";
    print
"</p><p>Please contact the owner or administrator of this application in order\n";
    print
"to obtain the necessary access if you want to use it.<\/p><p>Please include\n";
    print
"the URL of this application as well as the privilege code below in any\n";
    print "support request<\/p>.";
    print "</td>\n";
    $self->EndInnerRow();

    $self->StartInnerRow();
    print "<td align=center>Required Privilege:</td>\n";
    print "<td align=center>", $self->Encode($code), "</td>\n";
    $self->EndInnerRow();

    $self->EndInnerTable();

    $self->EndErrorBlockTable();
    $self->PageFooter();
    exit;
}

# Begin-Doc
# Name: Exit
# Type: method
# Description: closes page and then exits
# Syntax: $obj->Exit();
# End-Doc
sub Exit {
    my $self  = shift;
    my $error = shift;

    $self->_CloseNonPageBlocks();
    $self->PageFooter();
    exit;
}

# Begin-Doc
# Name: ErrorExit
# Type: method
# Description: prints an error msg in a block and exits
# Syntax: $obj->ErrorExit($errmsg);
# End-Doc
sub ErrorExit {
    my $self  = shift;
    my $error = shift;

    $self->_CloseNonPageBlocks();

    if ( $self->{_in_error_exit} ) {
        print "<!-- error exit recursed, terminating app -->";
        exit;
    }
    $self->{_in_error_exit} = 1;

    $self->StartErrorBlockTable( "Error Message", 600 );

    print "<center>";
    if ($error) {
        print $self->Encode($error);
    }
    print "</center>";

    $self->EndErrorBlockTable();
    $self->PageFooter();
    exit;
}

# Begin-Doc
# Name: ErrorWarn
# Type: method
# Description: prints an error msg in a block and continues, for warnings
# Syntax: $obj->ErrorWarn($errmsg);
# End-Doc
sub ErrorWarn {
    my $self  = shift;
    my $error = shift;

    $self->_CloseNonPageBlocks();

    $self->StartErrorBlockTable( "Error Warning", 600 );

    print "<center>";
    if ($error) {
        print $self->Encode($error);
    }
    print "</center>";

    $self->EndErrorBlockTable();
}

# Begin-Doc
# Name: ErrorWarnSQL
# Type: method
# Description: prints a sql error message and continues
# Syntax: $obj->ErrorWarnSQL($errmsg, [$db]);
# Comments: If $db is passed in, will use that to display last query and query arguments
# End-Doc
sub ErrorWarnSQL {
    my $self  = shift;
    my $error = shift;
    my $db    = shift;
    my $quiet = $self->{quiet};

    $self->ErrorSQLHelper( "Database Error (Warning)", $error, $db );
}

# Begin-Doc
# Name: ErrorExitSQL
# Type: method
# Description: prints a sql error and exits
# Syntax: $obj->ErrorExitSQL($errmsg, [$db]);
# Comments: If $db is passed in, will use that to display last query and query arguments
# End-Doc
sub ErrorExitSQL {
    my $self  = shift;
    my $error = shift;
    my $db    = shift;
    my $quiet = $self->{quiet};

    if ( $self->{_in_error_exit} ) {
        print "<!-- error exit recursed, terminating app -->";
        exit;
    }

    $self->{_in_error_exit} = 1;

    $self->_CloseNonPageBlocks();
    $self->ErrorSQLHelper( "Database Error", $error, $db );
    $self->PageFooter();
    exit;
}

# Begin-Doc
# Name: ErrorSQLHelper
# Type: method
# Description: prints a sql error message block
# Syntax: $obj->ErrorSQLHelper($blocktabletitle, $errmsg, [$db]);
# Comments: If $db is passed in, will use that to display last query and query arguments
# End-Doc
sub ErrorSQLHelper {
    my $self  = shift;
    my $title = shift;
    my $error = shift;
    my $db    = shift;
    my $quiet = $self->{quiet};

    $self->StartErrorBlockTable( $title, 600 );

    print "<center>";
    if ($error) {
        print $self->Encode($error);
    }

    if ( !$quiet ) {
        print "<br><font size=-1>";
        print
"<A HREF=\"javascript:document.getElementById('errorBlockDetails').className='errorBlockDetailsShow';void(0)\">Show Details</a> | ";
        print
"<A HREF=\"javascript:document.getElementById('errorBlockDetails').className='errorBlockDetailsHide';void(0)\">Hide Details</a>\n";
        print "</font>\n";
    }

    print "<div id=\"errorBlockDetails\" class=\"errorBlockDetailsHide\">";
    $self->StartBlockTable( "Database Error Details", 590 );
    $self->StartInnerTable();

    if ( !$quiet && $DBI::err ) {
        $self->StartInnerHeaderRow();
        print "<td colspan=2 align=center>Database Error Information</td>\n";
        $self->EndInnerHeaderRow();

        $self->StartInnerRow();
        print "<td><b>Error Code ($DBI::err)</b></td>\n";
        print "<td>", $self->Encode($DBI::errstr), "</td>\n";
        $self->EndInnerRow();
    }

    if ( !$quiet && $db ) {
        eval {
            my $last_qry    = $db->SQL_LastQuery();
            my $last_params = $db->SQL_LastParams();

            if ($last_qry) {
                $self->StartInnerHeaderRow();
                print "<td colspan=2 align=center>Database Query</td>\n";
                $self->EndInnerHeaderRow();

                $self->StartInnerRow();
                print "<td><b>Last Query</b></td>\n";
                print "<td>", $self->Encode($last_qry), "</td>\n";
                $self->EndInnerRow();

            }

            if ( ref($last_params) ) {
                my @params = @{$last_params};

                if ( $#params >= 0 ) {
                    $self->StartInnerHeaderRow();
                    print "<td colspan=2 align=center>Query Parameters</td>\n";
                    $self->EndInnerHeaderRow();

                    for ( my $i = 0 ; $i <= $#params ; $i++ ) {
                        $self->StartInnerRow();
                        print "<td colspan=2><b>$i: </b>\n";
                        print $self->Encode( $params[$i] ), "</td>\n";
                        $self->EndInnerRow();
                    }
                }
            }
        };
    }

    $self->EndInnerTable();
    $self->EndBlockTable();
    print "</div>";

    print "</center>\n";
    $self->EndErrorBlockTable();
}

# Begin-Doc
# Name: Encode
# Type: method
# Description: html encodes any special characters for safe output
# Syntax: $str = $obj->Encode($str);
# Comments: This routine should be used wherever possible. At this time it is only
# encoding the html bracket characters, though it could easily encode others and/or
# special characters/binary/etc.
# End-Doc
sub Encode {
    my $self = shift;
    my $txt  = shift;

    $txt =~ s/&/&amp;/gio;
    $txt =~ s/</&lt;/gio;
    $txt =~ s/>/&gt;/gio;
    $txt =~ s/"/&quot;/gio;
    $txt =~ s/\?/&#63;/gio;

    return $txt;
}

# Begin-Doc
# Name: URLEncode
# Type: method
# Description: Encodes a string in URL encoded format
# Syntax: $string = $obj->URLEncode($string)
# Comments: All chars other than [A-Za-z0-9-_] are converted to %XX hex notation
# End-Doc
sub URLEncode {
    my $self = shift;
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
# Name: StartErrorBlockTable
# Type: method
# Description: starts a error block table with a particular header caption
# Syntax: $obj->StartErrorBlockTable($title, [$width])
# End-Doc
sub StartErrorBlockTable {
    my $self = shift;

    $self->_push_block("ErrorBlockTable");
    print "<div id=\"errorBlock\">";
    $self->StartBlockTable(@_);
}

# Begin-Doc
# Name: StartBlockTable
# Type: method
# Description: starts a block table with a particular header caption
# Syntax: $obj->StartBlockTable($title, [$width])
# End-Doc
sub StartBlockTable {
    my $self  = shift;
    my $title = shift;
    my $width = shift;

    print "<table class=\"blockTable\" ";
    if ($width) {
        print " width=$width";
    }
    print ">\n";
    if ($title) {
        print "<tr><th class=\"blockTableHeader\">";
        print $self->Encode($title);
        print "</th></tr>\n";
    }
    print "<tr><td class=\"blockTableContent\">\n";

    $self->_push_block("BlockTable");
}

# Begin-Doc
# Name: StartInnerTable
# Type: method
# Description: starts an inner table, optionally with a particular set of column headers
# Syntax: $obj->StartInnerTable([@headers])
# Comments: If columns are specified, will put that many header columns on the interior table
# End-Doc
sub StartInnerTable {
    my $self    = shift;
    my @headers = @_;

    print "<table class=\"blockTableInner\">\n";

    if ( $#headers >= 0 ) {
        $self->StartInnerHeaderRow();
        foreach my $header (@headers) {
            my $th = $header;
            $th = $self->Encode($header);
            $th =~ s/&lt;br&gt;/<br>/gio;
            print "<th><b>${th}</b></th>\n";
        }
        $self->EndInnerHeaderRow();
    }

    $self->{inner_row_number} = 0;
    $self->_push_block("InnerTable");
}

# Begin-Doc
# Name: StartInnerRow
# Type: method
# Description: starts an inner row and manage tracking even/odd state for coloring
# Syntax: $obj->StartInnerRow()
# End-Doc
sub StartInnerRow {
    my $self = shift;

    print "<tr ";
    if ( $self->{inner_row_number} % 2 == 0 ) {
        print "class=\"blockTableInnerRowEven\"";
    }
    else {
        print "class=\"blockTableInnerRowOdd\"";
    }
    print ">";

    $self->{inner_row_number}++;
    $self->_push_block("InnerRow");
}

# Begin-Doc
# Name: StartInnerHeaderRow
# Type: method
# Description: starts an inner header row and reset the even/odd coloring state
# Syntax: $obj->StartInnerHeaderRow()
# End-Doc
sub StartInnerHeaderRow {
    my $self = shift;

    print "<tr class=\"blockTableInnerHeader\">";
    $self->_push_block("InnerHeader");
    $self->{inner_row_number} = 0;
}

# Begin-Doc
# Name: StartInnerHeaderCell
# Type: method
# Description: starts an inner header cell
# Syntax: $obj->StartInnerHeaderCell()
# End-Doc
sub StartInnerHeaderCell {
    my $self = shift;

    print "<td class=\"blockTableInnerHeader\">";
    $self->_push_block("InnerHeaderCell");
}

# Begin-Doc
# Name: EndInnerHeaderRow
# Type: method
# Description: ends an inner header row and manage tracking even/odd
# Syntax: $obj->EndInnerHeaderRow()
# End-Doc
sub EndInnerHeaderRow {
    my $self = shift;

    $self->_pop_block("InnerHeader");
    print "</tr>";
}

# Begin-Doc
# Name: EndInnerHeaderCell
# Type: method
# Description: ends an inner header cell
# Syntax: $obj->EndInnerHeaderCell()
# End-Doc
sub EndInnerHeaderCell {
    my $self = shift;

    $self->_pop_block("InnerHeaderCell");
    print "</td>";
}

# Begin-Doc
# Name: EndInnerRow
# Type: method
# Description: end a inner row
# Syntax: $obj->EndInnerRow()
# End-Doc
sub EndInnerRow {
    my $self = shift;

    $self->_pop_block("InnerRow");
    print "</tr>\n";
}

# Begin-Doc
# Name: EndBlockTable
# Type: method
# Description: end a block table
# Syntax: $obj->EndBlockTable()
# End-Doc
sub EndBlockTable {
    my $self = shift;

    $self->_pop_block("BlockTable");

    print "</td></tr></table>\n\n";
}

# Begin-Doc
# Name: EndErrorBlockTable
# Type: method
# Description: end an error block table
# Syntax: $obj->EndErrorBlockTable()
# End-Doc
sub EndErrorBlockTable {
    my $self = shift;

    $self->EndBlockTable();
    print "</div>";
    $self->_pop_block("ErrorBlockTable");
}

# Begin-Doc
# Name: EndInnerTable
# Type: method
# Description: end a inner table
# Syntax: $obj->EndInnerTable()
# End-Doc
sub EndInnerTable {
    my $self = shift;

    $self->_pop_block("InnerTable");
    print "</table>\n";
}

1;

