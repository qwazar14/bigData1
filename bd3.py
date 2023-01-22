from pyspark import SparkContext
from pyspark.sql import SparkSession
from graphframes import GraphFrame

sc = SparkContext("local", "GraphX Test")
spark = SparkSession(sc)

vertices = spark.createDataFrame([("name1", "Petro"), ("name2", "Kostiantyn"), ("name3", "Oleh"), ("name4", "Viktoriia"), ("name5", "Hanna"), ("name6", "Dmytro")], ["id", "name"])
edges = spark.createDataFrame([("name1", "name2", "is-friend-with"), ("name1", "name3", "wrote-status"), ("name2", "name4", "like-status"), ("name3", "name5", "is-friend-with"), ("name4", "name6", "wrote-status"), ("name5", "name1", "like-status")], ["src", "dst", "type"])
g = GraphFrame(vertices, edges)

# Get in-degree of each vertex
g.inDegrees.show()

# Filter vertices with in-degree greater than 1
g.vertices.filter("inDegree > 1").show()

# Find the number of "is-friend-with" connections in the graph
g.edges.filter("type = 'is-friend-with'").count()

sc.stop()
