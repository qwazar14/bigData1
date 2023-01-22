from pyspark.sql import SparkSession
from graphframes import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("GraphX OOP") \
    .getOrCreate()

# Define vertices
vertices = spark.createDataFrame([
    ("name1", "Petro"),
    ("name2", "Kostiantyn"),
    ("name3", "Oleh"),
    ("name4", "Viktoriia"),
    ("name5", "Hanna"),
    ("name6", "Dmytro")
], ["id", "name"])

# Define edges
edges = spark.createDataFrame([
    ("name1", "name2", "is-friend-with"),
    ("name1", "name3", "wrote-status"),
    ("name2", "name4", "like-status"),
    ("name2", "name5", "is-friend-with"),
    ("name3", "name6", "wrote-status")
], ["src", "dst", "relationship"])

# Create graph
myGraph = GraphFrame(vertices, edges)

# 1.2.2 Get the edges from the newly obtained graph
print(myGraph.edges.collect())

# 1.2.3 Use the triplets() method to combine vertices and edges based on VertexId.
print(myGraph.triplets.collect())