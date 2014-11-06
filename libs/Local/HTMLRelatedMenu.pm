#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#

=pod

Begin-Doc
Name: Local::HTMLRelatedMenu
Type: module
Description:  Allows for easy creation of linked popup menus in html
Syntax:  require Local::HTMLRelatedMenu;
Example:

my $x = new Local::HTMLRelatedMenu(
        basename => "pets",
        primaryname => "category",
        secondaryname => "choice");

$x->SetPrimaryChoices("indoor" => "Indoor Pets",
        "outdoor" => "Outdoor Pets",
);

$x->SetSecondaryChoices("indoor",
        "" => "",
        "cat-without-claws" => "Cat w/o Claws",
        "fish" => "Aquarium w/ Fish",
        "rabbit" => "Rabbit",
);

$x->SetSecondaryChoices("outdoor",
        "" => "",
        "cat-with-claws" => "Cat w/ Claws",
        "dog" => "Dog",
        "rabbit" => "Rabbit",
);

&HTMLStartForm("test");
print $x->Generate(primary_selected => "indoor", secondary_selected => "rabbit");
&HTMLSubmit("Go");
&HTMLEndForm;

End-Doc

=cut

use strict;

package Local::HTMLRelatedMenu;
require Exporter;
use Local::UsageLogger;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
@ISA    = qw(Exporter);
@EXPORT = qw();

BEGIN {
    &LogAPIUsage();
}

# Begin-Doc
# Name: new
# Type: function
# Description: Creates a HTMLRelatedMenu object
# Syntax: $ex = new Local::HTMLRelatedMenu(%options);
# Comments: %options are 'basename', 'primaryname', 'secondaryname' - which control field
# names used for the popup menusm. Also available are 'primary_selected', and 'secondary_selected',
# which let you choose the value (not label) that is to be selected by default. These can be
# overridden in the Generate method calls.
# End-Doc
sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;

    # set any object params
    my $tmp = {};
    $tmp->{"basename"}      = $opts{basename}      || "relmenu";
    $tmp->{"primaryname"}   = $opts{primaryname}   || "primary";
    $tmp->{"secondaryname"} = $opts{secondaryname} || "secondary";

    $tmp->{"primary_selected"}   = $opts{primary_selected};
    $tmp->{"secondary_selected"} = $opts{secondary_selected};

    $tmp->{primary_values}   = [];
    $tmp->{primary_labels}   = [];
    $tmp->{secondary_values} = {};
    $tmp->{secondary_labels} = {};

    &LogAPIUsage();

    return bless $tmp, $class;
}

# Begin-Doc
# Name: SetPrimaryChoices
# Type: method
# Description: Sets the choice list in the primary popup menu
# Syntax: $menu->SetPrimaryChoices($value1 => $label1, $value2 => $label2, ... );
# Comments: Values are passed in hash style, but are not put in a hash, so you can control
# the display order.
# End-Doc
sub SetPrimaryChoices {
    my $self = shift;
    my @info = @_;

    $self->{primary_values} = [];
    $self->{primary_labels} = [];

    while ( $#info >= 0 ) {
        my $v = shift @info;
        my $l = shift @info;
        push( @{ $self->{primary_values} }, $v );
        push( @{ $self->{primary_labels} }, $l );
    }
}

# Begin-Doc
# Name: SetSecondaryChoices
# Type: method
# Description: Sets the choice list in the secondary popup menu for a particular primary value
# Syntax: $menu->SetSecondaryChoices($primaryvalue, $value1 => $label1, $value2 => $label2, ... );
# Comments: If $primaryvalue is selected in the primary menu, will display this choice list in the
# secondary menu. Same behavior as SetPrimaryChoices.
# End-Doc
sub SetSecondaryChoices {
    my $self = shift;
    my $pval = shift;
    my @info = @_;

    $self->{secondary_values}->{$pval} = [];
    $self->{secondary_labels}->{$pval} = [];

    while ( $#info >= 0 ) {
        my $v = shift @info;
        my $l = shift @info;
        push( @{ $self->{secondary_values}->{$pval} }, $v );
        push( @{ $self->{secondary_labels}->{$pval} }, $l );
    }
}

# Begin-Doc
# Name: GenerateHead
# Type: method
# Description: Returns the html/javascript that should be placed in the HEAD of the document
# Syntax: print $menu->GenerateHead(%options);
# Comments: Optional %options can be passed in, same as the new method.
# End-Doc
sub GenerateHead {
    my $self = shift;
    my %opts = @_;

    my $res      = "";
    my $basename = $opts{basename} || $self->{basename} || "relmenu";
    my $priname  = $opts{primaryname} || $self->{primaryname} || "primary";
    my $secname
        = $opts{secondaryname}
        || $self->{secondaryname}
        || "secondary";

    if ( !$self->{procname} ) {
        $self->{procname} = $opts{procname} || "process_related_${basename}";
    }

    my $pname = $self->{procname};

    my $var_oe      = "prel_${basename}_oe";
    my $var_os      = "prel_${basename}_os";
    my $var_i       = "prel_${basename}_i";
    my $var_newlist = "prel_${basename}_newlist";

    $res .= "<SCRIPT LANGUAGE=\"JavaScript\">\n";
    $res .= "<!-- \n";

    $res
        .= "function $pname($var_oe,$var_os) {\n"
        . "\twith ($var_oe) {\n"
        . "\t\tfor (var $var_i=options.length-1;$var_i>0;$var_i--) options[$var_i]=null;\n" . "\t}\n";

    my ( $i, $j );
    my @pvals = @{ $self->{primary_values} };

    for ( $i = 0; $i <= $#pvals; $i++ ) {
        $res .= "\tif ($var_os==$i){\n";
        $res .= "\t\t$var_newlist = new Array(\n";

        my @svals = @{ $self->{secondary_values}->{ $pvals[$i] } };
        my @slabs = @{ $self->{secondary_labels}->{ $pvals[$i] } };

        for ( $j = 0; $j <= $#svals; $j++ ) {
            $res .= "\t\t\tnew Option(\"" . $slabs[$j] . "\",\"" . $svals[$j] . "\")";
            if ( $j != $#svals ) { $res .= ", "; }
            $res .= "\n";
        }

        $res .= "\t\t);\n\t}\n";
    }

    $res
        .= "\twith ($var_oe) {\n"
        . "\t\tfor (var $var_i=0;$var_i<$var_newlist.length;$var_i++) options[$var_i]="
        . $var_newlist
        . "[$var_i];\n"
        . "\t\toptions[0].selected=true;\n" . "\t}\n";

    $res .= "}\n";

    $res .= "//-->\n";
    $res .= "</SCRIPT>\n";

    return $res;
}

# Begin-Doc
# Name: GenerateBody
# Type: method
# Description: Returns the html for the select menus to be placed in BODY of page
# Syntax: print $menu->GenerateBody(%options);
# Comments: Optional %options can be passed in, same as the new method.
# End-Doc
sub GenerateBody {
    my $self = shift;
    my %opts = @_;

    my $res = "";

    my $basename = $opts{basename}    || $self->{basename}    || "relmenu";
    my $priname  = $opts{primaryname} || $self->{primaryname} || "primary";
    my $secname
        = $opts{secondaryname}
        || $self->{secondaryname}
        || "secondary";

    my $pname = $opts{procname} || $self->{procname};

    my $primary_selected   = $opts{primary_selected};
    my $secondary_selected = $opts{secondary_selected};

    if ( !defined($primary_selected) ) {
        $primary_selected = $self->{primary_selected};
    }
    if ( !defined($secondary_selected) ) {
        $secondary_selected = $self->{secondary_selected};
    }

    $res .= "<SELECT NAME=\"$priname\" onChange=\"$pname(this.form.$secname, this.selectedIndex)\">\n";

    my ( $i, $j );
    my @pvals = @{ $self->{primary_values} };
    my @plabs = @{ $self->{primary_labels} };

    for ( $i = 0; $i <= $#pvals; $i++ ) {
        $res .= "<OPTION VALUE=\"$pvals[$i]\"";
        if ( $pvals[$i] eq $primary_selected ) {
            $res .= " SELECTED";
        }
        $res .= ">$plabs[$i]\n";
    }

    $res .= "</SELECT>\n";

    $res .= "<SELECT NAME=\"$secname\">\n";

    # Default to first option
    if ( !$self->{secondary_values}->{$primary_selected} ) {
        $primary_selected = $pvals[0];
    }

    my @svals = @{ $self->{secondary_values}->{$primary_selected} };
    my @slabs = @{ $self->{secondary_labels}->{$primary_selected} };
    for ( $i = 0; $i <= $#svals; $i++ ) {
        $res .= "<OPTION VALUE=\"$svals[$i]\"";
        if ( $svals[$i] eq $secondary_selected ) {
            $res .= " SELECTED";
        }
        $res .= ">$slabs[$i]\n";
    }

    $res .= "</SELECT>\n";

    return $res;
}

# Begin-Doc
# Name: Generate
# Type: method
# Description: Returns both the head and body output together
# Syntax: print $menu->Generate(%options);
# Comments: Optional %options can be passed in, same as the new method. This method can be used
# if you don't care about strict HTML doctype compliance or want to have the entirety of the related
# menu inserted inline into your document.
# End-Doc
sub Generate {
    my $self = shift;
    return $self->GenerateHead(@_) . $self->GenerateBody(@_);
}

1;
