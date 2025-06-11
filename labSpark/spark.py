from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os

spark = SparkSession.builder \
    .appName("MapReduceExample") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

def inverted_index_example():
    if os.path.exists("docs"):
        documents = []
        for filename in os.listdir("docs"):
            with open(os.path.join("docs", filename), "r") as file:
                content = file.read()
                documents.append((filename, content))
    else:
        documents = [
            ("doc1.txt", "Hello world"),
            ("doc2.txt", "Hello Spark"),
            ("doc3.txt", "Spark is great for big data processing"),
            ("doc4.txt", "MapReduce is a programming model"),
            ("doc5.txt", "Spark can be used for MapReduce tasks")
        ]
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