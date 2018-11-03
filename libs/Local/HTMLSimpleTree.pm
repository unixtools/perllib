#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
#

=begin

Begin-Doc
Name: Local::HTMLSimpleTree
Type: module
Description: Simple ajax style tree builder with cookie enabled collapse/expand

Comments: Note - this tree object does require that each node in the tree must have 
a unique id.

Example:

use Local::HTMLSimpleTree;

my $tree = new Local::HTMLSimpleTree(
    id => "privsys-test1",
    icon_expanded => "/~privsys/icons/blue-minus.gif",
    icon_collapsed => "/~privsys/icons/blue-plus.gif",
    icon_empty => "/~privsys/icons/blue-solid.gif",
    icon_terminal => "/~privsys/icons/space-block.gif",
    callback => sub {
        my $id = shift;
        print "CB $id [$id]\n";
    }
);

$tree->add_node(id => "a");
$tree->add_node(id => "b");
$tree->add_node(id => "a:1", parent => "a");
$tree->add_node(id => "a:2", parent => "a", terminal => 1);
$tree->add_node(id => "a:1:A", parent => "a:1", terminal => 1);
$tree->add_node(id => "b:1", parent => "b");
$tree->add_node(id => "b:2", parent => "b");
$tree->add_node(id => "b:3", parent => "b", content => "Node with predefined content to bypass callback");
$tree->print();


End-Doc

=cut

package Local::HTMLSimpleTree;
require Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

use Local::UsageLogger;
use Carp;

@ISA    = qw(Exporter);
@EXPORT = qw(
);

=begin
Begin-Doc
Name: new
Type: method
Description: creates new HTMLSimpleTree object
Syntax: $obj = new Local::HTMLSimpleTree(id => $id, icon_WHICH => $uri, callback => $subref, [debug => 0/1], [jsdebug => 0/1]);
Comments: WHICH is "expanded", "collapsed", "empty", "terminal". 
Each has a server relative or absolute URL for the icon to use for that type of
tree node. Empty is a node with no children. Terminal is a node that specifically
cannot have any children. Tree will initially be fully visible. $id is an alphanumeric
string (starting with letter) that identifies this particular tree. It should
be unique across multiple uses of this API. The ID is used to form all callback
routine names, classes, object IDs, and cookie names. Callback is a subroutine
reference called with the id of each node, and is expected to output any node content
for that node. The Tree ID should include the application id to help guarantee
uniqueness. It should avoid generating <LI> tags or lists unless they are 
completely self contained.

If icons are not specified, will default to some ugly looking one from apache.

End-Doc
=cut

sub new {
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opts  = @_;
    my $tmp   = {};

    $tmp->{id} = $opts{id} || croak "must specify tree id";

    $tmp->{icon_terminal}  = "/icons/blank.gif";
    $tmp->{icon_expanded}  = "/icons/folder.open.gif";
    $tmp->{icon_collapsed} = "/icons/folder.gif";
    $tmp->{icon_empty}     = "/icons/folder.gif";

    foreach my $f ( "icon_expanded", "icon_collapsed", "icon_terminal", "icon_empty" ) {
        if ( defined( $opts{$f} ) ) {
            $tmp->{$f} = $opts{$f};
        }
    }

    if ( $opts{callback} ) {
        if ( ref( $opts{callback} ) ne "CODE" ) {
            croak "callback must be a subroutine reference";
        }
    }
    $tmp->{callback} = $opts{callback};

    $tmp->{jsdebug} = $opts{jsdebug};
    $tmp->{debug}   = $opts{debug};

    &LogAPIUsage();

    bless $tmp, $class;

    # initialize node structure
    $tmp->clear_nodes();

    return $tmp;
}

# Begin-Doc
# Name: gen_id
# Description: generates a new sequential id for a node with no id
# Syntax: $id = $obj->gen_id()
# End-Doc
sub gen_id {
    my $self = shift;
    my $seq  = int( $self->{unique_seq} ) + 1;
    my $tid  = $self->{id};

    my $id = sprintf( "%s-%.8d", $tid, $seq );

    $self->{unique_seq} = $seq;
    return $id;
}

# Begin-Doc
# Name: safe_id
# Description: munges/encodes id as needed to make it safe for DOM usage
# Syntax: $id = $obj->safe_id($id)
# Comments: for now, it just converts all non [A-Z0-9a-z-] to a dash
# Comments: this id needs to be DOM safe
# End-Doc
sub safe_id {
    my $self = shift;
    my $id   = shift;

    $id =~ s/[^A-Za-z0-9-]/-/go;
    $id =~ s/--+/-/go;
    if ( $id !~ /^[A-Za-z]+/o ) {
        $id = "id" . $id;
    }

    return $id;
}

# Begin-Doc
# Name: safe_treeid
# Description: munges/encodes id as needed to make it safe for DOM usage as a tree id
# Syntax: $id = $obj->safe_treeid($id)
# Comments: This id needs to be javascript function-name safe.
# End-Doc
sub safe_treeid {
    my $self = shift;
    my $id   = shift;

    $id =~ s/[^A-Za-z0-9]/_/go;
    if ( $id !~ /^[A-Za-z]+/o ) {
        $id = "id" . $id;
    }

    return $id;
}

# Begin-Doc
# Name: clear_nodes
# Description: Removes all nodes from current tree
# Syntax: $obj->clear_nodes();
# End-Doc
sub clear_nodes {
    my $self = shift;

    # Tree structure - root node just like any other node, but no ID
    $self->{data} = {};

    # Maintain a lookup table to each node by id for fast detection
    # of duplicates and quickly adding children by id
    $self->{id_to_node} = {};
}

# Begin-Doc
# Name: add_node
# Description: Add a node to current tree
# Syntax: $id = $obj->add_node([parent => $parent_node_id], [id => "node id"], [content => "node content"]);
# Comments: node id must be unique throughout the tree, if node id isn't passed
# in, will assign an return unique id. If an id is passed in, will return that id. If content is
# passed as a parameter, the 'print' method will output that as the node content and will not
# invoke the callback function for the node.
# End-Doc
sub add_node {
    my $self = shift;
    my %opts = @_;

    my $parent  = $opts{parent};
    my $id      = $opts{id};
    my $content = $opts{content};

    if ( !defined($id) ) {
        $id = $self->gen_id();
    }

    if ( !defined($id) ) {
        croak "no node id found";
    }

    if ( $self->{id_to_node}->{$id} ) {
        croak "unique ID constraint violated!";
    }

    my $pnode;
    if ($parent) {
        $pnode = $self->{id_to_node}->{$parent};
        if ( !$pnode ) {
            croak "non-existent parent ($parent) specified!";
        }
    }
    else {

        # top of tree
        $pnode = $self->{data};
    }

    if ( ref($pnode) ne "HASH" ) {
        croak "internal error - tree node not hash";
    }

    my $newnode = {};
    $newnode->{id} = $id;
    if ( $opts{terminal} ) {
        $newnode->{terminal} = 1;
    }
    else {
        $newnode->{children} = [];
    }
    if ($content) {
        $newnode->{content} = $content;
    }

    $self->{id_to_node}->{$id} = $newnode;
    push( @{ $pnode->{children} }, $newnode );

    return $id;
}

# Begin-Doc
# Name: print_js
# Description: helper routine to dump out the javascript for the tree implementation
# Syntax: $self->print_js();
# Comments: This is included inline instead of an external js file to make this module self contained.
# Comments: Future improvement - allow specifying a URL of another script to retrieve this from with caching/expires header
# End-Doc
sub print_js {
    my $self = shift;
    my $tid  = $self->safe_treeid( $self->{id} );

    my $icon_expanded  = $self->{icon_expanded};
    my $icon_collapsed = $self->{icon_collapsed};

    my $fn_icon_expanded = $icon_expanded;
    $fn_icon_expanded =~ s|.*/||go;
    my $fn_icon_collapsed = $icon_collapsed;
    $fn_icon_collapsed =~ s|.*/||go;

    my $cookie = "HST_Status_${tid}";

    my $nodebug = "//";
    if ( $self->{jsdebug} ) {
        $nodebug = "";
    }

    print "<script language=\"JavaScript\">\n";
    print <<EOJS;

function HST_SC_${tid}( name, value )
{
    var today = new Date();
    var expires_date = new Date( today.getTime() + (1000*30*24*60*60) );
    document.cookie = name + "=" +escape( value ) + ";expires=" + expires_date.toGMTString();
    HST_DBG_${tid}("Cookie($cookie) value is: " + value);
}

function HST_GC_${tid}( check_name ) {
    // first we'll split this cookie up into name/value pairs
    // note: document.cookie only returns name=value, not the other components
    var a_all_cookies = document.cookie.split( ';' );
    var a_temp_cookie = '';
    var cookie_name = '';
    var cookie_value = '';
    var b_cookie_found = false; // set boolean t/f default f
    
    for ( i = 0; i < a_all_cookies.length; i++ )
    {
        // now we'll split apart each name=value pair
        a_temp_cookie = a_all_cookies[i].split( '=' );
        
        // and trim left/right whitespace while we're at it
        // the dollar-sign is escaped for output from perl
        cookie_name = a_temp_cookie[0].replace(/^\\s+|\\s+\$/g, '');
    
        // if the extracted name matches passed check_name
        if ( cookie_name == check_name )
        {
            b_cookie_found = true;
            // we need to handle case where cookie has no value but exists (no = sign, that is):
            if ( a_temp_cookie.length > 1 )
            {
                // the dollar-sign is escaped for output from perl
                cookie_value = unescape( a_temp_cookie[1].replace(/^\\s+|\\s+\$/g, '') );
            }
            // note that in cases where cookie is initialized but no value, null is returned
            HST_DBG_${tid}("Cookie($cookie) value is: " + cookie_value);
            return cookie_value;
            break;
        }
        a_temp_cookie = null;
        cookie_name = '';
    }
    if ( !b_cookie_found )
    {
        return null;
    }
}               

function HST_CO_${tid}(myid)
{
   HST_DBG_${tid}("Collapse (" + myid + ")");

    var imgid = myid + "-img";

    var y = document.getElementById(imgid);
    var curimg = y.src;
    
    var listid = myid + "-ul";
    var d = document.getElementById(listid);

    if ( curimg.match("${fn_icon_expanded}") )
    {
        y.src = "${icon_collapsed}";
        d.style.display = "none";


        var cook = HST_GC_${tid}("$cookie");
        if ( ! cook )
        {
            cook = "";
        }

        var collapsed = cook.split(",");
        var j=0;
        var i;
        for(i = 0; i < collapsed.length; i++){
            if ( collapsed[i] == myid )
            {
                j++;
            }
        }
        if ( ! cook )
        {
            cook = myid;
        }
        else if ( myid && j==0 )
        {
            cook = cook + "," + myid;
        }
        HST_SC_${tid}("$cookie", cook);
    }
}

function HST_EX_${tid}(myid)
{
   HST_DBG_${tid}("Expand (" + myid + ")");

    var imgid = myid + "-img";

    var y = document.getElementById(imgid);
    var curimg = y.src;
    
    var listid = myid + "-ul";
    var d = document.getElementById(listid);

    if ( curimg.match("${fn_icon_collapsed}") )
    {
        y.src = "${icon_expanded}";
        d.style.display = "";

        var cook = HST_GC_${tid}("$cookie");
        if ( ! cook )
        {
            cook = "";
        }

        var collapsed = cook.split(",");
        var j=0;
        var newcook = "";
        var i;
        for(i = 0; i < collapsed.length; i++){
            if ( collapsed[i] != myid && collapsed[i] && collapsed[i] != "undefined" )
            {
                if ( newcook != "" && newcook != "," )
                {
                    newcook = newcook + ",";
                }
                newcook = newcook + collapsed[i];
            }
        }
        HST_SC_${tid}("$cookie", newcook);
    }
}

function HST_TGL_${tid}(x)
{
    var myid = x.id;
    HST_DBG_${tid}("Toggle (" + myid + ")");

    var imgid = myid + "-img";

    var y = document.getElementById(imgid);
    var curimg = y.src;
    
    if ( curimg.match("${fn_icon_expanded}") )
    {
        HST_CO_${tid}(myid);
    }
    else if ( curimg.match("${fn_icon_collapsed}" ) )
    {
        HST_EX_${tid}(myid);
    }
}

function HST_RES_${tid}()
{
    HST_DBG_${tid}("Restore");

    var cook = HST_GC_${tid}("$cookie");
    if ( ! cook )
    {
        cook = "";
    }

    var collapsed = cook.split(",");
    var len = collapsed.length;
    var i;
    HST_DBG_${tid}("number of collapsed entries in cookie: " + len);
    HST_DBG_${tid}("collapsed cookie: " + cook);
    for(i = 0; i < collapsed.length; i++){
        var id = collapsed[i];
        if ( id != "" )
        {
            if ( document.getElementById(id) ) {
             HST_DBG_${tid}("restoring collapse of: " + id);
             HST_CO_${tid}(id);
            } else {
             HST_DBG_${tid}("node (" + id + ") not present in DOM");
            }
        }
    }

    var topul = document.getElementById("HST_${tid}_topul");
    if ( topul )
    {
        topul.style.display="";
    }
}

function HST_DBG_${tid}(x)
{
    $nodebug var debugdiv = document.getElementById("HST_${tid}_debug");
    $nodebug debugdiv.innerHTML = x + "<br>\\n" + debugdiv.innerHTML;
}



EOJS
    print "</script>\n";

}

# Begin-Doc
# Name: print
# Description: print out the current tree, including stylesheets and javascript
# Syntax: $obj->print();
# Comments: will call the callback return in turn for each node of the tree
# End-Doc
sub print {
    my $self = shift;
    my $tid  = $self->safe_treeid( $self->{id} );

    print "<style type=\"text/css\">\n";
    print ".HST_${tid}_listclass li { list-style-type: none; }\n";
    print ".HST_${tid}_listclass img { border: none; }\n";
    print "</style>";

    $self->print_js();

    print "<ul id=\"HST_${tid}_topul\" class=\"HST_${tid}_listclass\" style=\"display:none\">\n";
    my $base = $self->{data};

    my @children = ();
    if ( ref($base) eq "HASH" && ref( $base->{children} ) eq "ARRAY" ) {
        @children = @{ $base->{children} };
    }
    foreach my $cnode (@children) {
        $self->print_helper( $tid, $cnode, 0 );
    }
    print "</ul>\n";
    print "<div id=\"HST_${tid}_debug\"></div>\n";

    print "<script language=\"JavaScript\">HST_RES_${tid}();</script>\n";
}

# Begin-Doc
# Name: print_helper
# Type: method
# Description: recursive routine called for each node in tree to generate output, calls the callback routine
# Syntax: $self->print_helper($safe_treeid, $noderef, $depth)
# End-Doc
sub print_helper {
    my $self  = shift;
    my $tid   = shift;
    my $node  = shift;
    my $depth = shift;

    return if ( !$node );

    return if ( ref($node) ne "HASH" );

    my $id       = $node->{id};
    my $terminal = $node->{terminal};
    my $debug    = $self->{debug};

    my $sid;
    if ( !defined($id) ) {
        $sid = "root";
    }
    else {
        $sid = "node-" . $self->safe_id($id);
    }

    my $indent;
    if ($debug) {
        $indent = " " x $depth;
    }

    my @children;
    if ( ref( $node->{children} ) eq "ARRAY" ) {
        @children = @{ $node->{children} };
    }

    if ($terminal) {
        my $icon_terminal = $self->{icon_terminal};

        $debug && print "${indent}<!-- terminal node $sid -->\n";
        print "${indent}<li><img src=\"$icon_terminal\">";
    }
    else {
        $debug && print "${indent}<!-- non-terminal node $sid -->\n";
        print "${indent}<li><a id=\"hst-${tid}-$sid\" onClick=\"HST_TGL_${tid}(this);\">";

        my $icon = $self->{icon_expanded};
        if ( scalar(@children) == 0 ) {
            $icon = $self->{icon_empty};
        }

        print "<img id=\"hst-${tid}-$sid-img\" src=\"$icon\"></a>";
    }

    my $content = $node->{content};
    if ($content) {
        if ( ref($content) eq "SCALAR" ) {
            print $$content;
        }
        else {
            print $content;
        }
    }
    else {
        my $cb = $self->{callback};
        if ($cb) {
            $cb->($id);
        }
    }

    print "</li>\n";

    if ( !$terminal ) {
        $debug && print "${indent}<!-- children of node $sid -->\n";
        print "${indent}<ul id=\"hst-${tid}-$sid-ul\">\n";
        foreach my $cnode (@children) {
            $self->print_helper( $tid, $cnode, $depth + 1 );
        }
        print "${indent}</ul>\n";
    }
}

1;

