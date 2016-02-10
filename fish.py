#!/Users/LoicMDIVAD/Lmdapp/products/anaconda/bin/python
# coding: utf-8

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
    FISHING_NET = []
    profile = os.path.abspath(os.environ['HOME'] + '/.bash_profile')
    fishfile = os.path.abspath(os.environ['HOME'] + '/.fish_profile')

    with open(profile, 'r') as bash:
        for line in bash:
            if linePattern('comment', line) or line == '\n' or isPS1(line):
                continue
            elif linePattern('export', line):
                FISHING_NET.append(line.replace('export', 'set -x').replace('=', ' '))
            elif linePattern('alias', line):
                FISHING_NET.append(line.replace('=', ' '))
            elif linePattern('PATH', line):
                FISHING_NET.append(                     line.replace('export PATH=', 'set -gx PATH ')                     .replace(':$PATH', ' $PATH')                 )
            else:
                pass

    with open(fishfile, 'w') as f:
        for l in FISHING_NET:
            f.write(l)

    print 'fish export available at ~/.fish_profile.'
