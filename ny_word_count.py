from pyspark import SparkContext

sc = SparkContext("local[3]", "word count")
sc.setLogLevel("ERROR")

lines = sc.textFile("./in/word_count.text")

# Using ReduceByKey
words = lines.flatMap(lambda line: line.split(" "))\
                .map(lambda x: (x.lower(), 1))\
                .reduceByKey(lambda x, y: x+y)

# Using sortBy
sortedwords = words.sortBy(lambda wordcount: wordcount[1], ascending=False)  # sort by count
for word, count in sortedwords.collect():
    print("{} : {}".format(word, count))

words_flipped = words.map(lambda x: (x[1], x[0]))
words_flipped_max = words_flipped.max()   # returns a tuple [(81, 'the')]

print('The most repeated words is \"{}\", repeating {} times'.format(words_flipped_max[1], words_flipped_max[0]))

# To see all the words and their count
wordsflat = lines.flatMap(lambda line: line.split(" "))
wordcounts = wordsflat.countByValue()  # creates a dict
for word, count in wordcounts.items():
    print("{} : {}".format(word, count))


