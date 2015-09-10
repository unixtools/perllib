=begin

Begin-Doc
Name: Local::SiteTemplate
Type: module
Description: wrapper around apptemplate to set defaults

This particular module isn't intended to be used directly, but rather should be overridden
in the library search path with a site specific library that sets appropriate defaults.

End-Doc

=cut

package Local::SiteTemplate;
require 5.000;
use Exporter;
use strict;
use Local::AppTemplate;

use vars qw (@ISA @EXPORT);
@ISA    = qw(Local::AppTemplate Exporter);
@EXPORT = qw();

=begin
Begin-Doc
Name: new
Type: method
Description: creates object
Syntax: $obj->new(%params)
Comments: Same syntax/behavior as routine in AppTemplate module.
End-Doc
=cut

sub new {
    my $self = shift;
    my @args = @_;

    return $self->SUPER::new(@_);
}
