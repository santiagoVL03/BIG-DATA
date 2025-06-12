from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
import re

spark = SparkSession.builder \
    .appName("MapReduceExample") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

def inverted_index_example():
    documents = []
    local_path = "/home/hduser/BIG-DATA/labSpark/docs"

    for filename, content in sc.wholeTextFiles(local_path).collect():
        short_filename = os.path.basename(filename)
        documents.append((short_filename, content))

    rdd = sc.parallelize(documents)

    # Limpieza de palabras y eliminación de duplicados por archivo
    inverted_index = rdd.flatMap(lambda x: [(word.lower(), x[0]) 
                    for word in set(re.findall(r'\b[a-zA-Z]+\b', x[1]))]) \
                        .groupByKey() \
                        .mapValues(lambda docs: sorted(set(docs)))  # elimina archivos duplicados por palabra
    
    return inverted_index.collect()

if __name__ == "__main__":
    print("Inverted Index Example:")
    inverted_index_result = inverted_index_example()
    for word, docs in sorted(inverted_index_result):
        print(f"{word}: {', '.join(docs)}")
    spark.stop()
