#!/bin/bash

grails package-plugin

rm -rf dist
mkdir -p dist

mv *.zip dist
cd dist && unzip *.zip
rm *.zip
