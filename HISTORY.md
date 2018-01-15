# CIHM::TDR module history

## Short version

The git log suggests this code was created on November 22, 2017, and that it was created by Russell McOrmond.

This was the date that code was refactored while it was in our Subversion repository, and included many 'svn mv' commands from across our repository to land in CIHM-TDR/trunk .

When we tried to use `git svn` to move to Git to publish on Github, we lost the history prior to moving into the new project. We tried many different ways to extract the history, including a perl script to filter the output of `svnadmin dump` , but the problem turned out to be too messy.  We decided to leave that history in the Subversion repository, and create this note to reference the issue.

As of January 12, 2018 when the internal http://svn.c7a.ca/svn/c7a/ repository is at revision 6786

This repository was created using:

`git svn clone file:///data/svn/c7a -T CIHM-TDR/trunk --authors-file=/home/git/authors.txt --no-metadata -s CIHM-TDR`

## Longer version

This module has history dating back to early 2012 when William Wueppelmann was creating tools for the newly designed TDR, which was intended to replace the ealier 'cmr' repository (and related tools).

By early 2013, Robert Schmidt was doing most of the back-end platform work at Canadiana, and was making the largest contributions to this codebase.

In early 2014 Robert updated the command-line tools to use MooseX::App . Robert left Canadiana in the summer of 2014.

CIHM::TDR::Repository was started in November 2014, and represents when Russell McOrmond became the primary developer for this module.

Minor updates were made by Darcy Quesnel in 2015 and Julienne Pascoe in 2016.
