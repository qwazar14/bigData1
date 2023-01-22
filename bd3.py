from pyspark.sql import SparkSession
from graphframes import *

# Initialize SparkSession

spark = SparkSession.builder \
    .appName("GraphX OOP") \
    .getOrCreate()
spark.sparkContext.addPyFile("path-to-graphframes-jar")

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

# 1.2.5 Determine the number of edges coming out of each vertex
print(myGraph.aggregateMessages(lambda msg: msg.sendToSrc(1), lambda a, b: a + b).collect())

# 1.2.6 For each vertex number, display its name and the number of edges coming out of it
print(myGraph.aggregateMessages(lambda msg: msg.sendToSrc(1), lambda a, b: a + b).join(myGraph.vertices).collect())

# 1.2.7 Remove the vertex number, leaving only its name and the number of edges coming out of it
print(myGraph.aggregateMessages(lambda msg: msg.sendToSrc(1), lambda a, b: a + b).join(myGraph.vertices).map(
    lambda x: (x[1][1], x[1][0])).collect())

# 1.2.8 show all the names of the vertices (even those from which no edges coming out of it)
print(myGraph.aggregateMessages(lambda msg: msg.sendToSrc(1), lambda a, b: a + b).rightOuterJoin(myGraph.vertices).map(
    lambda x: (x[1][1], x[1][0])).collect())

# 1.2.9 Let's clean up the previous listing by getting rid of Some and None.
print(myGraph.aggregateMessages(lambda msg: msg.sendToSrc(1), lambda a, b: a + b).rightOuterJoin(myGraph.vertices).map(
    lambda x: (x[1][1], x[1][0].getOrElse(0))).collect())
