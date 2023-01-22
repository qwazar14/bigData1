from pyspark.sql import SparkSession
from graphframes import *


class Graph:
    def __init__(self):
        # Start a Spark session
        self.spark = SparkSession.builder.appName("GraphX").getOrCreate()

    def create_graph(self):
        # Create a DataFrame for the vertices
        vertices = self.spark.createDataFrame(
            [("name1", "Petro"), ("name2", "Kostiantyn"), ("name3", "Oleh"), ("name4", "Viktoriia"), ("name5", "Hanna"),
             ("name6", "Dmytro")], ["id", "name"])

        # Create a DataFrame for the edges
        edges = self.spark.createDataFrame([("name1", "name2", "is-friend-with"), ("name1", "name3", "wrote-status"),
                                            ("name2", "name4", "like-status"), ("name3", "name5", "is-friend-with"),
                                            ("name4", "name6", "wrote-status"), ("name5", "name1", "like-status")],
                                           ["src", "dst", "type"])

        # Create a GraphFrame
        myGraph = GraphFrame(vertices, edges)
        return myGraph

    def get_edges(self, myGraph):
        return myGraph.edges.collect()

    def get_triplets(self, myGraph):
        return myGraph.triplets.collect()

    def add_annotation(self, myGraph):
        myGraph = myGraph.mapTriplets(
            lambda t: (t.attr, t.attr == "is-friends-with" and t.srcAttr.name.lower().contains("a")))
        return myGraph

    def out_degree(self, myGraph):
        out_degree = myGraph.aggregateMessages(sendToSrc=lambda e: 1, add=lambda a, b: a + b)
        return out_degree.collect()

    def vertex_name_out_degree(self, myGraph):
        vertex_name_out_degree = out_degree.join(myGraph.vertices)
        return vertex_name_out_degree.collect()

    def remove_vertex_number(self, vertex_name_out_degree):
        return vertex_name_out_degree.map(lambda x: (x[1][1]["name"], x[1][0])).collect()

    def rightOuterJoin(self, vertex_name_out_degree):
        vertex_name_out_degree = out_degree.rightOuterJoin(myGraph.vertices)
        return vertex_name_out_degree.map(lambda x: (x[1][1]["name"], x[1][0])).collect()

    def getOrElse(self, vertex_name_out_degree):
        return vertex_name_out_degree.map(lambda x: (x[1][1]["name"], x[1][0].getOrElse(0))).collect()

    def propagateEdgeCount(self, myGraph):
        # Initialize the graph
        graph = myGraph.mapVertices((_, _) => 0)
        # propagate edge count
        graph = graph.aggregateMessages(sendToDst=lambda e: 1, add=lambda a, b: a + b)
        return graph.vertices.collect()
    def pregel(self,myGraph):
        g = myGraph.mapVertices((vid,vd) => 0).pregel(0, activeDirection = EdgeDirection.Out)(
            (id,vd,a) => math.max(vd,a),
            (et) => Iterator((et.dstId, et.srcAttr+1)),
            (a,b) => math.max(a,b))
        return g.vertices.collect()

    def shortest_path(self,myGraph):
        results = ShortestPaths.run(myGraph, Array(3))
        return results.vertices.collect()

# create an object of the class
graph_obj = Graph()
myGraph = graph_obj.create_graph()
print("Edges: ",graph_obj.get_edges(myGraph))
print("Triplets: ",graph_obj.get_triplets(myGraph))
print("Annotation added: ",graph_obj.add_annotation(myGraph))
print("Out degree: ",graph_obj.out_degree(myGraph))
print("Vertex name and out degree: ",graph_obj.vertex_name_out_degree(myGraph))
print("Remove vertex number: ",graph_obj.remove_vertex_number(myGraph))
print("Right outer join: ",graph_obj.rightOuterJoin(myGraph))
print("Get or else: ",graph_obj.getOrElse(myGraph))
print("Propagate edge count: ",graph_obj.propagateEdgeCount(myGraph))
print("Pregel: ",graph_obj.pregel(myGraph))
print("Shortest path: ",graph_obj.shortest_path(myGraph))