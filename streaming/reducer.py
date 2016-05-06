#!/usr/bin/env python
import sys

current_key = None
sum = 0 
for line in sys.stdin:
	key, value = line.strip().split("\t",1)
	if key != current_key:
		if current_key:
			print "%s\t%d" % (current_key, sum)
		current_key = key
		sum =0
	sum += int(value)
if current_key:
	print "%s\t%d" % (current_key, sum)