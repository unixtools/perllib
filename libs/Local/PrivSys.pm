#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T/UMRPerl library
#

=begin

Begin-Doc
Name: Local::PrivSys
Type: module
Description: Stub no-op module

Override with another UsageLogger in search path to activate. Be sure and list the 'use lib' for override
dir after the use lib for perllib itself.

End-Doc

=cut

package Local::PrivSys;
require Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw(
    PrivSys_RequirePriv
    PrivSys_QuietRequirePriv
    PrivSys_FetchPrivs
);

BEGIN {
    &LogAPIUsage();
}

sub PrivSys_RequirePriv {
    &LogAPIUsage();
    die;
}

sub PrivSys_QuietRequirePriv {
    &LogAPIUsage();
    die;
}

sub PrivSys_FetchPrivs {
    &LogAPIUsage();
    return ();
}

1;

