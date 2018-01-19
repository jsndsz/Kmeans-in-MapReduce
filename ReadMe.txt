Java code: seedPoints.java is used to generate centroids. PointGenerator.java is used to
generate Points. Problem3a.java, Problem3b.java, Problem3c.java, Problem3d.java are for
task1, task2, task3, task4 and Problem3dea.java, Problem3deb.java are for task5.
Step1 Creation of Datasets
For generating K centroids: for example, we give 10 points.
> java seedPoints 10
> java PointGenerator
Then we get our datasets: Kseeds.txt, PointsLarge.txt.
Upload to HDFS:
> hadoop dfs -put Kseeds.txt /
> hadoop dfs -put PointsLarge.txt /


Mapper Class:
The Mapper class reads in the list of Points and calculates the distance between each centroid
and the point. It assigns the point to the lowest Euclidian distance between the point and the
centroid. This is then written out to the reducer.
Reducer Class:
The Reducer class aggregates all the points associated with a centroid into the list and writes out
the centroid and the list into hdfs.
Driver:
In the first loop, the Kmeans file is fed into the Distributed Cache. During the second loop
onwards the previous file written out by the Reducer, which contains the centroid points is fed
into the Distributed Cache. This creates the iterative loop.





