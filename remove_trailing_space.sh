#!bin/bash

files=$1

if [ -z "$files" ]; then
  echo "Please enter file name(s), e.g. sh replace_trailing_space.sh \"./*\""
else
  echo $files
fi


for f in $files; do
  vim -c "%s/\s\+$//e" -c "wq" $f
done
