#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
# This module contains portions copyright Curators of the University of Missouri.
#

# Begin-Doc
# Name: Local::AppMenu
# Type: module
# Description: object to manage to create a simple application dropdown menu
# End-Doc

=cut

Example config:

my $appmenu = new Local::AppMenu(
    prefix => "myapp",
    menus  => [
        {   name       => "File",
            link       => "optionalurl",
            visible_cb => sub { return 1 },
            id         => "file",
            items      => [
                {   name       => "Open",
                    link       => "https://www.mst.edu",
                    visible_cb => sub { return 1 },
                },
                {   name       => "Hidden",
                    link       => "https://www.yahoo.com",
                    visible_cb => sub { return 0 },
                },
                {   name       => "Close",
                    link       => "https://www.google.com",
                    visible_cb => sub { return 1 },
                }
            ]
        },
        {   name       => "Edit",
            visible_cb => sub { return 1 },
            id         => "edit",
            items      => [
                {   name       => "Undo",
                    link       => "https://www.mst.edu",
                    visible_cb => sub { return 1 },
                },
                {   name       => "Cut",
                    link       => "https://www.yahoo.com",
                    visible_cb => sub { return 0 },
                },
                {   name       => "Copy",
                    link       => "https://www.google.com",
                    visible_cb => sub { return 1 },
                }
            ]
        }
    ]
);

print $appmenu->style();

print $appmenu->menuhtml();

print $appmenu->linkhtml();

Config notes:
link and visible_cb are optional - if visible_cb is present will be called at time of display to determine
if item or menu should be shown

=cut

package Local::AppMenu;
require Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

use Local::UsageLogger;
use Local::Encode;

@ISA    = qw(Exporter);
@EXPORT = qw();

BEGIN {
    &LogAPIUsage();
}

# Begin-Doc
# Name: new
# Type: function
# Description: Creates an object
# Syntax: $netinfo = new Local::AppMenu(%opts)
# Comments: Pass in keys 'prefix' and 'menus' with menu configuration array ref
# End-Doc
sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;

    &LogAPIUsage();

    my $tmp = {};

    $tmp->{prefix} = $opts{prefix};
    $tmp->{menus}  = $opts{menus};

    if ( ref( $tmp->{menus} ) ne "ARRAY" ) {
        die "Must pass in menu array.";
    }

    return bless $tmp, $class;
}

# Begin-Doc
# Name: css
# Type: method
# Description: Returns CSS for the given menu
# Syntax: $css = $obj->css();
# End-Doc
sub css {
    my $self = shift;
    &LogAPIUsage();

    my $css    = "";
    my $prefix = $self->{prefix};
    my $menus  = $self->{menus};

    $css .= <<EOBASE;
.$prefix-am-btn {
    background: none;
    border: solid 0.1px #dddddd;
    color: black;
    font-size: .8rem;
    font-weight: bold;
    min-width: 8rem;
    padding-top: 0.4rem;
    padding-bottom: 0.3rem;
    padding-left: 1rem;
    padding-right: 1rem;
    text-align: left;
}
.$prefix-am {
    display: inline-block;
    position: relative;
}
.$prefix-am-items {
    background-color: #f1f1f1;
    display: none;
    min-width: 8rem;
    position: absolute;
    text-align: left;
    z-index: 1;
}
.$prefix-am-items a {
    background: white;
    border-bottom: 1px solid #adadad;
    border-left: 0.1px solid #eeeeee;
    border-right: 0.1px solid #eeeeee;
    color: #858583;
    display: block;
    font-family: sans-serif;
    font-size: 0.8rem;
    padding: 0.5rem 0.5rem 0.5rem 0.8rem;
    text-decoration: none;
}
EOBASE

    my $idx = 0;
    foreach my $mref (@$menus) {
        $idx++;

        $css .= ".$prefix-am-items-$idx a:hover {background-color: #f6eece}\n";
        $css .= ".$prefix-am-$idx:hover .$prefix-am-btn-$idx {background-color: #eeeeee;}\n";
        $css .= ".$prefix-am-$idx:hover .$prefix-am-items-$idx {display: block;}\n";
    }

    return $css;
}

# Begin-Doc
# Name: style
# Type: method
# Syntax: $html = $obj->style();
# Description: Returns stylesheet, a wrapper around CSS call with added style tags
# End-Doc
sub style {
    my $self = shift;
    return "<style>" . $self->css() . "</style>\n";
}

# Begin-Doc
# Name: menuhtml
# Type: method
# Syntax: $html = $obj->menuhtml([active => "id_or_name"]);
# Description: Returns HTML for menu, optionally pass in an "active" value to
#  indicate which submenu is active for highlighting
# End-Doc
sub menuhtml {
    my $self = shift;
    my %opts = @_;

    &LogAPIUsage();

    my $html   = "";
    my $prefix = $self->{prefix};
    my $menus  = $self->{menus};
    my $active = $opts{active};

    my $idx = 0;
    foreach my $mref (@$menus) {
        $idx++;

        my $vis = $mref->{visible_cb};
        if ( ref($vis) eq "CODE" ) {
            next if ( !&$vis() );
        }

        $html .= "<div class=\"$prefix-am $prefix-am-$idx\">\n";
        $html .= " <button class=\"$prefix-am-btn $prefix-am-btn-$idx\" ";
        if ( $mref->{link} ) {
            $html .= " onclick=\"location.href='" . $mref->{link} . "';\"";
        }
        $html .= ">" . $mref->{name} . "</button>\n";
        $html .= " <div class=\"$prefix-am-items $prefix-am-items-$idx\">\n";

        foreach my $iref ( @{ $mref->{items} } ) {

            my $vis = $iref->{visible_cb};
            if ( ref($vis) eq "CODE" ) {
                next if ( !&$vis() );
            }

            my $item = "<a href=\"";
            if ( $iref->{link} ) {
                $item .= $iref->{link};
            }
            else {
                $item .= "#";
            }
            $item .= "\">" . $iref->{name} . "</a>";
            $html .= "  " . $item . "\n";
        }
        $html .= " </div>\n";
        $html .= "</div>\n";
    }

    return $html;
}

# Begin-Doc
# Name: linkhtml
# Type: method
# Syntax: $html = $obj->linkhtml(active => "id_or_name");
# Description: Returns HTML for menu, passes in an "active" value to
#  indicate which submenu is active to determine which set of subitems to return
# End-Doc
sub linkhtml {
    my $self = shift;
    my %opts = @_;

    &LogAPIUsage();

    my $html   = "";
    my $prefix = $self->{prefix};
    my $menus  = $self->{menus};
    my $active = $opts{active};

    my $posthtml;
    foreach my $mref (@$menus) {
        my $vis = $mref->{visible_cb};
        if ( ref($vis) eq "CODE" ) {
            next if ( !&$vis() );
        }

        my @items;
        foreach my $iref ( @{ $mref->{items} } ) {

            my $vis = $iref->{visible_cb};
            if ( ref($vis) eq "CODE" ) {
                next if ( !&$vis() );
            }

            my $item = "<a href=\"";
            if ( $iref->{link} ) {
                $item .= $iref->{link};
            }
            else {
                $item .= "#";
            }
            $item .= "\">" . $iref->{name} . "</a>";
            $html .= "  " . $item . "\n";

            push( @items, $item );
        }

        if ( $active && ( $active eq $mref->{id} || $active eq $mref->{name} ) ) {
            $posthtml = join( " &middot; ", @items );
        }
    }
    return $posthtml;
}

1;
