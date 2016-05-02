#!/bin/env python
# coding: utf-8

""" 
Convertion of bash into fish profile.
Author : Loic M. Divad
Date : 12 December 2015
Revised: 02 May 2016
please see https://github.com/DivLoic/Argol
""" 
import os
import sys

# config

def isPS1(line):
    return True if line.find('PS1') != -1 else False

def linePattern(cmd, line):
    if cmd == 'comment':
        return True if line[0] == '#' else False
    elif cmd == 'export':
        if (line.find('PATH') != -1): return False
        return True if line.split(' ')[0] == 'export' else False
    elif cmd == 'alias':
        return True if line.split(' ')[0] == 'alias' else False
    elif cmd == 'PATH':
        return True if line.find('PATH') != -1 else False
    pass

# process

if __name__ == '__main__':

    profile = os.path.abspath(os.environ['HOME'] + '/.bash_profile')
    fishfile = os.path.abspath(os.environ['HOME'] + '/.fish_profile')

    with open(profile, 'r') as bash:
        
        with open(fishfile, 'w') as f:
            
            for line in bash:
                if linePattern('comment', line) or line == '\n' or isPS1(line):
                    continue
                elif linePattern('export', line):
                    f.write(line.replace('export', 'set -x').replace('=', ' '))
                elif linePattern('alias', line):
                    f.write(line.replace('=', ' '))
                elif linePattern('PATH', line):
                    f.write(
                        line.replace('export PATH=', 'set -gx PATH ')
                        .replace(':$PATH', ' $PATH')
                    )
                else:
                    pass

    print 'fish export available at ~/.fish_profile.'
