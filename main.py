from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext



def word_count(file,word):
  try:
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(file)
    sc.stop()
    return lines.filter(lambda line: word in line).count()
  except Exception as e:
    sc.stop()
    print (e)
    exit(1)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    print (lines.filter(lambda line: "Spark" in line).count())
    print ("!!!! Spark Count !!!!")
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    sc.stop()
