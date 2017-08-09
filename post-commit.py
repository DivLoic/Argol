#!/usr/bin/python
# coding: utf-8

"""
simple function to create a custum git-hook.
Author : Loic M. Divad
Date : 19 Apr 2017
please see https://github.com/DivLoic/notebook/blob/master/git-hooks/pre-commit.ipynb
"""

import fileinput
import subprocess

GITHUB_USER="DivLoic"

### functions
def applyBranch(line, branch_name, pattern="{{branch}}"):
    return line.replace(pattern, branch_name)

def unapplyBranch(line, branch_name, pattern="{{branch}}"):
    return line.replace(branch_name, pattern)

def editRmd(file_name, branch_name, func):
    stream = fileinput.FileInput(file_name, inplace=True)
    for line in stream:
        print func(line, branch_name),
        
    stream.close()
        
### process
BRANCH = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).strip()

editRmd("README.md", BRANCH, unapplyBranch)

print "[life-cycle]: use the git hook post-commit to update the README."
