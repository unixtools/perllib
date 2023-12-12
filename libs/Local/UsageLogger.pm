#
# This file is licensed under the Perl Artistic License 2.0 - http://www.opensource.org/licenses/artistic-license-2.0
# Development copy of this package available from: https://github.com/unixtools/perllib
# Cross contributions/development maintained in parallel with Missouri S&T Perl library
#

=begin

Begin-Doc
Name: Local::UsageLogger
Type: module
Description: Stub no-op module to do usage tracking

Override with another UsageLogger in the search path to activate. Be sure and list the 'use lib' for override
dir after the use lib for perllib itself.
End-Doc

=cut

package Local::UsageLogger;
require Exporter;
use strict;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

@ISA    = qw(Exporter);
@EXPORT = qw(
    LogAPIUsage
    ResetAPIUsage
);

# Begin-Doc
# Name: LogAPIUsage
# Type: subroutine
# Description: track api usage
# Access: public
# Syntax: &LogAPIUsage($msg)
#	Optional $msg will be recorded along with usage tracking
# Comments: Should be called from anywhere you want to track usage
# End-Doc
sub LogAPIUsage {
    return undef;
}

# Begin-Doc
# Name: ResetAPIUsage
# Type: subroutine
# Description: reset api usage logger caching
# Access: public
# Syntax: &ResetAPIUsage()
# Comments: Should be called when state should be reset
# End-Doc
sub ResetAPIUsage {
    return undef;
}

1;

