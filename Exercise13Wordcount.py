#
# DataScience BHTB MIM 12 S18
# Exercise 13
# 
# Martina Ahlswede
#
#
# most of this script is derived from script shown as an example during lecture ;)
#

from pyspark import SparkContext, SparkConf
from operator import add


if __name__ == "__main__":
    conf = SparkConf().setAppName("word count").setMaster("local[3]")
    sc = SparkContext(conf = conf)

    lines = sc.textFile("t8.shakespeare.txt")

    words = lines.flatMap(lambda line: line.split(" "))

    wordCounts = words.countByValue()

    ## unable to sort here :(/
    #wordCounts = words.countByValue().sortBy(_._2)
    #sorted = wordCounts.items().sortBy(lambda a: a[1])

    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))

    # sort the output
    sortedWordCounts = sorted(wordCounts.items(), key=lambda x: (x[1],x[0]))

    print("- . - . - . - . - . - . - . - . - . - . -")
    #print (len(sortedWordCounts))

    print("the most used words in t8.shakespeare.txt\n")
    for i in range(1, 25):
        print(i, ":", sortedWordCounts[-i])

