#!/usr/bin/env python -u
#

__author__ = 'bwarminski'

# Borrowed from subprocess.py and extended to capture error output for stderr
class CalledProcessError(Exception):
    """This exception is raised when a process run by check_call() or
    check_output() returns a non-zero exit status.
    The exit status will be stored in the returncode attribute;
    check_output() will also store the output in the output attribute.
    """
    def __init__(self, returncode, cmd, output=None, err=None):
        self.returncode = returncode
        self.cmd = cmd
        self.output = output
        self.err = err
    def __str__(self):
        return "Command '%s' returned non-zero exit status %d" % (self.cmd, self.returncode)
