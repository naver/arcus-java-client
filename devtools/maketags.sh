#!/bin/sh
find ../src -name "*java" > ./cscope.files
ctags --extra=+q -L ./cscope.files -f ./tags
rm ./cscope.files
