import sys

for line in sys.stdin:
	input = line.strip()
	input = input.split('\t')
	userID = input[0]
	if len(input) == 1:
		continue

	friends = input[1].split(",")
	for idx in range(len(friends)):
		friend = friends[idx]
		if friend == userID:
			continue
		user = ""
		if int(userID) < int(friend):
			user = userID + "," + friend
		else:
			user = friend + "," + userID
		
		f = friends[:idx]+friends[idx+1:]
		f = ",".join(f)
		print("%s\t%s"%(user, f))

