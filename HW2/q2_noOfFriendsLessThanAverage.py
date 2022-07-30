
from unittest import result
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("NumberOfFriendsLessThanAverage")
sc = SparkContext(conf = conf)

def parseLine(line):
    input = line.split('\t')
    userId = input[0]
    if len(input) == 1:
        return
    friends = input[1].split(',')
    res = []
    for idx, friend in enumerate(friends):
        pair = ""
        if not len(userId) or not len(friend) or userId == ' ' or friend == ' ':
            continue
        if int(userId) < int(friend):
            pair = userId + " " + friend
        else:
            pair = friend + " " + userId
        res.append((pair, set(friends[:idx] + friends[idx + 1:])))
    return res

def intersection(l):
    lists = []
    for list in l:
        lists.append(list)
    
    return len(set(lists[0]).intersection(set(lists[1])))

def sortFunc(x):
    keys = x[0].split()
    return (int(keys[0]), int(keys[1]))
 
lines = sc.textFile("file:///Users/adhutsav/Desktop/BigData/HW2/input/mutual.txt")
"""
    Steps :
        1. Create a flatMap where each line input is converted to "<userA> <userB>" to a value of list of friends. 
        2. GroupByKey : so that "<userA> <userB>" would have common friends as list. 
        3. Filter out if there is only one element in the list. 
        4. Compute the intersection using mapValues. 
        5. filter out the friends that have 0 common friends.
        6. Calculate the average. 
        7. And then filter out based on the average that is computed. 
"""
rdd = lines.flatMap(parseLine).groupByKey().filter(lambda x: len(x[1])>1).mapValues(intersection)

average = rdd.map(lambda x : ('*', (x[1], 1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda x : x[0] / x[1]).collect()

lessThanAverage = rdd.filter(lambda x : float(x[1]) < float(average[0][1])).sortBy(sortFunc).collect()

with open ("q2_output.txt", "w") as f:
    f.write(f"Average is : {average[0][1]}\n")
    for key, value in lessThanAverage:
        keys = key.split()
        f.write(f"{keys[0]},{keys[1]}\t{value}\n")
