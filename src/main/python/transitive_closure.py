import sys
from random import Random

from pyspark import SparkContext

num_edges = 5
num_vertices = 500
rand = Random(3)

def generate_graph():
    edges = set()
    while len(edges) < num_edges:
        src = (rand.randrange(0, num_vertices))
        dst = (rand.randrange(0, num_vertices))
        if src != dst:
            edges.add((src,dst))
    return edges

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print >> sys.stderr, "Usage: transitive closure <master> [<slices>]"
        exit(1)
    sc = SparkContext(sys.argv[1], "TransitiveClosure")
    slices = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    closure = sc.parallelize(generate_graph(), slices).cache()

    old_count = 0
    new_count = closure.count()

    new_num = 1
    
    while new_num != 0:
        joint_edges = closure.join(edges)
        new_num = joint_edges.count()
        new_edges = joint_edges.map(lambda (_,(a,b)): (b,a))
        new_closure = closure.union(new_edges).distinct()
        
        closure.unpersist()
        closure = new_closure
        edges = 
        

    #while new_count != old_count:
    #    joint_edges = closure.join(edges)
    #    new_edges = joint_edges.map(lambda (_, (a,b)): (b,a))
    #    new_closure = closure.union(new_edges).distinct().count()

    #    closure.unpersist()
    #    closure = new_closure
    #closure.unpersist()
    #edges.unpersist()

    print "TC has %i edges" % closure.count()
