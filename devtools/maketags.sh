#!/bin/sh
find ../src -name "*java" > ./cscope.files
ctags --extras=+q -L ./cscope.files -f ./tags
rm ./cscope.files
