#!/bin/bash
# List added lines in commit 444fe416a whose visual width (tab=4) exceeds 90.
cd /home/onurctirtir/citus-iso/msparallel-1210/src
git show 444fe416a | awk '
/^\+\+\+/ { next }
/^\+/ {
  line = substr($0, 2)
  n = 0
  for (i = 1; i <= length(line); i++) {
    c = substr(line, i, 1)
    if (c == "\t") { n += 4 } else { n++ }
  }
  if (n > 90) { printf "%d cols | %s\n", n, line }
}
'
