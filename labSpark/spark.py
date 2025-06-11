from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os

spark = SparkSession.builder \
    .appName("MapReduceExample") \
    .master("yarn") \
    .getOrCreate()

sc = spark.sparkContext

def inverted_index_example():
    documents = []
    hdfs_path = "hdfs://master:9000/docs"
    for filename in sc.wholeTextFiles(hdfs_path).collect():
        documents.append((filename[0], filename[1]))
    rdd = sc.parallelize(documents)
    
    inverted_index = rdd.flatMap(lambda x: [(word, x[0]) for word in x[1].split()]) \
                        .groupByKey() \
                        .mapValues(list)
                        
    return inverted_index.collect()

if __name__ == "__main__":
    print("Inverted Index Example:")
    inverted_index_result = inverted_index_example()
    for word, docs in inverted_index_result:
        print(f"{word}: {', '.join(docs)}")
    spark.stop()