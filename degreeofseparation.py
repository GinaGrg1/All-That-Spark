"""
x = '5983 1165 3836 4361 1282'.split()
conn = [int(connection) for connection in x[1:]]
conn ==> [1165, 3836, 4361, 1282]
Here we are trying to find the degree of separation between Spiderman & hero with id 14
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DegreesOfSeparation").getOrCreate()
sc = spark.sparkContext

startCharacterID = 5306  # Spiderman
targetCharacterID = 14  # some random superhero

hitCounter = sc.accumulator(0)


def converttobfs(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = [int(connection) for connection in fields[1:]]

    color = 'WHITE'
    distance = 9999

    if heroID == startCharacterID:
        color = 'GREY'
        distance = 0

    return heroID, (connections, distance, color)   # 5983, ([1165,3836,4361,1282], 9999, 'WHITE')


def createstartingrdd():
    inputfile = sc.textFile("./marvel-graph10.txt")
    return inputfile.map(converttobfs)


def bfsmap(node):  # 5983 ([1165, 3836, 4361, 5861, 5485], 9999, 'WHITE')
    characterID = node[0]  # 5983
    data = node[1]  # ([1165, 3836, 4361, 5861, 5485], 9999, 'WHITE')
    connections = data[0]  # [1165, 3836, 4361, 5861, 5485]
    distance = data[1]   # 9999
    color = data[2]     # 'GRAY'

    results = []

    # If this node needs to be expanded
    if color == 'GRAY':
        for connection in connections:
            newcharacterId = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if targetCharacterID == connection:
                hitCounter.add(1)
            newEntry = (newcharacterId, ([], newDistance, newColor))
            results.append((newEntry))

        # We have processed this node, so color it black
        color = 'BLACK'

    # Emit the input node so we don't lose it
    results.append((characterID, (connections, distance, color)))
    return results


def bfsreduce(data1, data2):
    edges1, distance1, color1 = data1[0],  data1[1],  data1[2]
    edges2, distance2, color2 = data2[0],  data2[1],  data2[2]

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections. If so, preserve them
    if len(edges1) > 0:
        edges.extend(edges1)
    if len(edges2) > 0:
        edges.extend(edges2)

    # Preserve minimum distance
    if distance1 < distance:
        distance = distance1
    if distance2 < distance:
        distance = distance2

    # Preserve darkest color
    if color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK'):
        color = color2
    if color1 == 'GRAY' and color2 == 'BLACK':
        color = color2

    if color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK'):
        color = color1
    if color2 == 'GREY' and color1 == 'BLACK':
        color = color1

    return edges, distance, color


# Main program
iterationRDD = createstartingrdd()  # 5983 ([1165, 3836, 4361, 5861, 5485], 9999, 'WHITE')
print(iterationRDD.take(3))

for iteration in range(10):  # max 10 degrees of separation
    print("Running BFS iteration# " + str(iteration + 1))

    mapped = iterationRDD.flatMap(bfsmap)  # returns
    print(mapped.take(3))
    print("Processing " + str(mapped.count()) + " values.")
    # we are calling count() so that it does an action and hitCounter is set

    if hitCounter.value > 0:
        print("Hit the target character! From " + str(hitCounter.value) + " different directions. ")
        break

    # Reducer combines data for each character ID, preserving the darkest color and the shortest path.
    iterationRDD = mapped.reduceByKey(bfsreduce)
