#!/bin/bash
#
# Make sure you've installed codox!
#
# lein plugin install codox 0.6.1

git checkout develop
lein doc
git add doc
git commit -m "Update code docs via gendoc.sh script."
git push origin develop
git checkout gh-pages
git checkout develop -- doc
git commit -m "Update code docs via gendoc.sh script."
git push -u origin gh-pages
