import sys


for line in sys.stdin:
	line = line.strip()
	input = line.split("\t")
	if len(input) != 2 or not input[0] or not input[1]:
		continue

	friendList = []
	for ll in input[1]:
		friendList.append(ll)
	
	mutual = []
	common = set()
	for ff in friendList[0]:
		common.add(ff)
	
	for ff in friendList[1]:
		if ff in common:
			mutual.append(ff)
	
	print("%s\t%s"%(input[0], mutual))
