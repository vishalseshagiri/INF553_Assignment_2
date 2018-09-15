# Sample execution command bin/spark-submit FirstName_LastName_SON.py <casenumber> <csvfilepath> <support>

# Problem 1 deliverables

# FirstName_LastName_SON_Small2.case1-3.txt
# Time Taken 19.9087021351
# minPartitions = 2

# FirstName_LastName_SON_Small2.case2-5.txt
# Time Taken 
# minPartitions = 2

# Problem 2 deliverables

# FirstName LastName SON MovieLens.Small.case1-120.txt
# Time Taken 81.9662539959
# minPartitions = 8

# FirstName LastName SON MovieLens.Small.case1-150.txt
# Time Taken 13.4684081078
# minPartitions = 8

# FirstName LastName SON MovieLens.Small.case2-180.txt
# Time Taken 176.821056843
# minPartitions = 8

# FirstName LastName SON MovieLens.Small.case2-200.txt
# Time Taken 90.3010289669
# mainPartitions = 8

# Problem 3 deliverables

# FirstName LastName SON MovieLens.Big.case1-30000.txt
# Time Taken 713.825545788
# minPartitions = 8

# FirstName LastName SON MovieLens.Big.case1-35000.txt
# Time Taken 197.660165071
# minPartitions = 8

# FirstName LastName SON MovieLens.Big.case2-2800.txt
# Time Taken 950.547194958
# minPartitions = 8

# FirstName LastName SON MovieLens.Big.case2-3000.txt
# Time Taken 612.930243015
# minPartitions = 8


from operator import add
from pyspark import SparkContext
import itertools
import sys
import math
import time
import os

def mapPhase1Remix(splitIndex, iterator):
	
	global support_threshold
	global number_of_transactions
	
	movieCounts = {}
	moviePairCounts = {}
	frequentSingletons = {}
	frequentPairs = {}
	longestTransaction = 0
	
	baskets = list(iterator)
	
	adjusted_support_threshold = int(math.ceil(len(baskets) / float(number_of_transactions) * support_threshold))

	# if splitIndex == 0:
	# 	print("Adjusted_Threshold {}".format(adjusted_support_threshold))

	# Singletons
	
	for userId, movieIdList in baskets:
		longestTransaction = longestTransaction if longestTransaction > len(movieIdList) else len(movieIdList)
		for movie in movieIdList:
			# movieCounts.update({movie : movieCounts.setdefault(movie, 0) + 1})
			if not movieCounts.get(movie):
				movieCounts[movie] = 1
			else:
				movieCounts[movie] += 1
							
	for movie, count in movieCounts.iteritems():
		if count >= adjusted_support_threshold:
			# frequentSingletons.update({movie : 1}) # {F, 1}
			frequentSingletons[movie] = 1
			yield movie
	
					
	# Pairs
	
	for combination in itertools.combinations(sorted(frequentSingletons.keys()), 2):
		for userId, movieIdList in baskets: 
			# if set(combination).issubset(movieIdList):
			if all(x in movieIdList for x in combination):
				# moviePairCounts.update({combination : moviePairCounts.setdefault(combination, 0) + 1})
				if not moviePairCounts.get(combination):
					moviePairCounts[combination] = 1
				else:
					moviePairCounts[combination] += 1
									
	previous_frequent_tuples = {}
	for moviePair, count in moviePairCounts.iteritems():
		if count >= adjusted_support_threshold:
			# frequentPairs.update({moviePair : 1}) # {F, 1}
			# previous_frequent_tuples.setdefault(moviePair[:-1], []).append(moviePair[-1])
			if not previous_frequent_tuples.get(moviePair[:-1]):
				previous_frequent_tuples[moviePair[:-1]] = [moviePair[-1]]
			else:
				previous_frequent_tuples[moviePair[:-1]].append(moviePair[-1])

			yield moviePair

	# n-tuples
							
	current_n = 3
	while(current_n <= longestTransaction and len(previous_frequent_tuples) > 0):
		movie_n_tuple_counts = {}
			
		# if splitIndex == 0:
		# 	print("{}-tuple count {}".format(current_n - 1, len(previous_frequent_tuples)))

		for key, value in previous_frequent_tuples.iteritems():
			# value = sorted(value)
			value.sort()
			for combination in itertools.combinations(value, 2):
				combination = key + combination
				for userId, movieIdList in baskets: 
					# if set(combination).issubset(set(movieIdList)):
					if all(x in movieIdList for x in combination):
						# movie_n_tuple_counts.update({combination : movie_n_tuple_counts.setdefault(combination, 0) + 1})
						if not movie_n_tuple_counts.get(combination):
							movie_n_tuple_counts[combination] = 1
						else:
							movie_n_tuple_counts[combination] += 1

								
		previous_frequent_tuples = {}
		for movieTuple, count in movie_n_tuple_counts.iteritems():
			if count >= adjusted_support_threshold:
				# previous_frequent_tuples.setdefault(movieTuple[:-1], []).append(movieTuple[-1])
				if not previous_frequent_tuples.get(movieTuple[:-1]):
					previous_frequent_tuples[movieTuple[:-1]] = [movieTuple[-1]]
				else:
					previous_frequent_tuples[movieTuple[:-1]].append(movieTuple[-1])
				yield movieTuple
						
		current_n += 1
        
def mapPhase2(splitIndex, iterator):
	
	global candidate_itemsets
	
	baskets = list(iterator)

	for candidate in candidate_itemsets:
		if type(candidate) != tuple:
			candidate = (candidate,)
		count = 0
		for userId, movieIdList in baskets:
			# if set(candidate).issubset(set(movieIdList)):
			if all(x in movieIdList for x in candidate):
				count += 1
		yield candidate, count

def saveToFile(result):
	global case_number
	global csv_file_path
	global support_threshold

	d = {}

	for cand in result:
		if not d.get(len(cand)):
			d[len(cand)] = [cand]
		else:
			d[len(cand)].append(cand)
		# d.setdefault(len(cand), []).append(cand)

	count = 0

	# FirstName_LastName_SON_Small2.case1-3.txt
	# data_string = csv_file_path.split("/")[-1].split(".")[0]
	data_string = ".".join(csv_file_path.split(os.sep)[-1].split(".")[-3:][:-1])
	with open("Vishal_Seshagiri_SON_{}.case{}-{}.txt".format(data_string, case_number, support_threshold), "w") as f:
		first_flag = True
		for key, value in d.items():
			if first_flag:
				first_flag = False
			else:
				f.write("\n\n")

			count += len(value)
			f.write("{}".format(str(sorted(value)).strip('[]').replace(',)', ')')))

if __name__ == "__main__":

	# Sample execution command bin/spark-submit FirstName_LastName_SON.py <casenumber> <csvfilepath> <support>
	# Data/Small1.csv
	# start_time = time.time()
	
	case_number = int(sys.argv[1])
	csv_file_path = sys.argv[2]
	support_threshold = int(sys.argv[3])

	sc = SparkContext(appName="Assignment2")
	data = sc.textFile("{}".format(csv_file_path), minPartitions=2)

	header = data.first()
	data = data.filter(lambda row : row != header)

	if case_number == 1:
		transactions = data.map(lambda x: [int(i) for i in x.split(',')[:2]])
	else:
		transactions = data.map(lambda x: [int(i) for i in reversed(x.split(',')[:2])])

	transactions = transactions.groupByKey().map(lambda x: (x[0],sorted(set(list(x[1])))))

	number_of_transactions = transactions.countApprox(1, 0.2)

	number_of_partitions = transactions.getNumPartitions()

	candidate_itemsets = transactions.mapPartitionsWithIndex(mapPhase1Remix).distinct().collect()
	result = transactions.mapPartitionsWithIndex(mapPhase2).reduceByKey(add).filter(lambda x: x[1] >= support_threshold).map(lambda x : x[0]).collect()

	saveToFile(result)

	# end_time = time.time()
	# print("Time taken {}".format(end_time - start_time))