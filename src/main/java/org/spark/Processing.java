package org.spark;


import org.apache.spark.broadcast.Broadcast;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.regex.Pattern;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import org.types.VertexDegree;

/**
 * Processing for the DataSet's RDD Representation.
 */
public class Processing {
    private static final Pattern SPACE = Pattern.compile("[ \\t\\x0B\\f\\r]+");

    //Reused results.
    private static Broadcast<Long> overallTriangleCount;
    private static JavaPairRDD<Long, long[]> fonl;

    //Overall Computation------------------------------------------------------------------------

    /**
     * Compute All the correlation coefficients, for each vertex.
     * @param  lines A RDD that contains the graph as lines.
     * @param  addReverse Add the reverse of each vertex if True.
     * @return            Each vertex with its correlation coefficient as RDD.
     */
    public static JavaPairRDD<Long, Float> compute(JavaRDD<String> lines, JavaSparkContext sc, boolean addReverse) {

        // Do cleanup/prprocessing of input.
        JavaPairRDD<Long, Long> edges =  lines.filter((line)->{
            String currentLine = line.toString();
            if(!currentLine.startsWith("#")) return true;
            return false;
        }).flatMapToPair(s->{ // Prepare input for main logic, adges llist.
            String[] x = SPACE.split(s);
            long source = Long.parseLong(x[0]);
            long target = Long.parseLong(x[1]);
            ArrayList<Tuple2<Long, Long> > list = new ArrayList<Tuple2<Long, Long> >();
            list.add(new Tuple2<Long, Long>(source, target));
            if(addReverse) list.add(new Tuple2<Long, Long>(target, source));
            return list.iterator();
        });

        //Create the neighbor list for each vertex.
        JavaPairRDD<Long, long[]> neighbors = createNeighborList(edges);

        //Compute triangles...
        //Create FONL, reused.
        fonl = createFonl(neighbors).cache();
        // It is reused, add default storage level to Disk if data is too big.

        //Create Candidates list
        JavaPairRDD<Long, long[]> candidateList = createCandidates(fonl);

        //Combine FONL and Candidates list to compute the Tiangle count.
        JavaPairRDD<Long, Long> vertexTriangleCount =  createVertexTC(fonl, candidateList).cache();

        //Use the above triangle count list to get the overall count. (Broadcast as read-only result)
        overallTriangleCount = sc.broadcast(computeOverallTriangleCount(vertexTriangleCount));

        //COMPUTE THE COEFFICIENT
        //List of vertices with their Local Correlation coefficients
        return computeLcc(vertexTriangleCount, fonl).persist(StorageLevel.MEMORY_AND_DISK());
        // Have data ready for further use.
    }

    //COMPUTE THE GLOBAL COEFFICIENT
    // Works based on Cached results, called after compute.
    public static Broadcast<Float> computeGlobal(JavaSparkContext sc){
        return sc.broadcast(computeGCC(overallTriangleCount.value(), fonl));
    }

    // Algorithm Parts -----------------------------------------------------------------------

    /**
     * Create neighbor list for each vertex.
     * @param  edges Edges pair RDD (key=source, value=destination)
     * @return       An RDD of vertex and an array of neighboring vertices.
     */
    public static JavaPairRDD<Long, long[]> createNeighborList(JavaPairRDD<Long, Long> edges){
        return edges.groupByKey().mapToPair(t -> {
            LongSet set = new LongOpenHashSet();
            for (Long v : t._2) {
                set.add(v.longValue());
            }
                return new Tuple2<Long, long[]>(t._1, set.toLongArray());
            });
    }

    /**
     * Create the Filtered Ordered Neighbor List.
     * @param  neighborList List of Neighbors (value) to each vertex (key), as a pair RDD.
     * @return        Filtered Ordered Neighbor List as pair RDD (key=vertex, value=FONL)
     */
    public static JavaPairRDD<Long, long[]> createFonl(JavaPairRDD<Long, long[]> neighborList) {

        return neighborList.flatMapToPair(t -> {
            long deg = (long) t._2.length;
            if (deg == 0)  return Collections.emptyIterator();

            VertexDegree vd = new VertexDegree(t._1, deg);
            List<Tuple2<Long, VertexDegree> > degreeList = new ArrayList<Tuple2<Long, VertexDegree> >((int) deg);

            // Add degree information of the current vertex to its neighbor
            for (long neighbor : t._2) {
                degreeList.add(new Tuple2<>(neighbor, vd));
            }
            return degreeList.iterator();
        }).groupByKey().mapToPair(v -> {
            // Iterate over neighbors to calculate degree of the current vertex
            long degree = 0;
            if (v._2 == null) return new Tuple2<>(v._1, new long[] {0});
            for (VertexDegree vd : v._2) degree++;

            //Filter list: If target vertex degree is higher than the current source.
            List<VertexDegree> list = new ArrayList<>();
            for (VertexDegree vd : v._2)
                if (vd.getDegree() > degree || (vd.getDegree() == degree && vd.getVertex() > v._1))
                    list.add(vd);

            //Sort Neighbors List based on degree.
            Collections.sort(list, (a, b) -> {
                long x, y;
                if (a.getDegree() != b.getDegree()) {
                    x = a.getDegree();
                    y = b.getDegree();
                } else {
                    x = a.getVertex();
                    y = b.getVertex();
                }
                long ret = x - y;
                return (int) ret;
            });

            // FONL list.
            long[] fonlList = new long[list.size() + 1];
            fonlList[0] = degree;
            for (int i = 1; i < fonlList.length; i++)
                fonlList[i] = list.get(i - 1).getVertex();
            return new Tuple2<>(v._1, fonlList);
        });
    }

    /**
     * Create Candidates List for each vertex. Effectively transform the FONL to
     * Edges (2 vertices) wich many be part of a triangle + higher degree neighbors
     * of both vertices.
     * @param  fonl RDD of FONL (value) for each vertex (key).
     * @return      RDD of key=vertex and value=(vertex or FONL key, List of candidates).
     */
    public static JavaPairRDD<Long, long[]> createCandidates(JavaPairRDD<Long, long[]> fonl) {
        return fonl.filter(t -> t._2.length > 2) // Select vertices having more than 2 items in their values
            .flatMapToPair(t -> {
                long size = t._2.length - 1; // the first index holds the node's degree, hence -1

                // If edge (2 vertices) has no common neighbors...
                if (size == 1) return Collections.emptyIterator(); //do not put in candidate list.

                List<Tuple2<Long, long[]> > output = new ArrayList<Tuple2<Long, long[]> >((int) size);
                for (int index = 1; index < size; index++) {
                    int len = (int) size - index;
                    long[] cvalue = new long[len + 1];
                    cvalue[0] = t._1; // First vertex in the triangle
                    System.arraycopy(t._2, index + 1, cvalue, 1, len);
                    Arrays.sort(cvalue, 1, cvalue.length); // quickSort
                    output.add(new Tuple2<>(t._2[index], cvalue));
                }
                return output.iterator();
            });
    }

    /**
     * Count the overall amount of triangles in the graph.
     * Contains ACTION!
     * @param  vertexTCList List of vertices with triangle count, as PairRDD.
     * @return              Overall amount of triangles in the graph
     */
    public static long computeOverallTriangleCount(JavaPairRDD<Long, Long> vertexTCList) {
        Long triangleCountX3 = vertexTCList.map(kv -> Long.valueOf(kv._2)).reduce((a, b) -> a + b);
        return triangleCountX3 / 3;
    }

    /**
     * Compute Triangle Count of each vertex in the graph. Use the FONL and The
     * Candidates list to get the triangle count for each of the filtered vertices.
     * Contains ACTION!
     * @param  fonl       Filtered ordered neighbor list.
     * @param  candidates
     * @return            key=vertex, value=number of trinagles as pair RDD.
     */
    public static JavaPairRDD<Long, Long> createVertexTC(JavaPairRDD<Long, long[]> fonl, JavaPairRDD<Long, long[]> candidates ){
        return candidates.cogroup(fonl)
           .flatMapToPair(kv -> {
             // Iterate over the common vertices from fonl.
              Iterator<long[]> iterator = kv._2._2.iterator();
              List<Tuple2<Long, Long> > output = new ArrayList<>();
              // If no common vertices, return...
              if (!iterator.hasNext())  return output.iterator();
              // otherwize keep the array from FONL.
              long[] hDegs = iterator.next();

              //Iterate over the tuples from candidates list..
              iterator = kv._2._1.iterator();
              // If no common vertices, return...
              if (!iterator.hasNext()) return output.iterator();
              //Sort the array from FONL.
              Arrays.sort(hDegs, 1, hDegs.length);
              long sum = 0;
              do { // For each of the key vertices.
                  //Find the common higher degree neghibors in order to form the triangles.
                  long[] forward = iterator.next();
                  long count = sortedIntersection(hDegs, forward, output, 1, 1);
                  //Sorted intersection effectively finds the triangles and counts them.
                  if (count > 0) {
                      sum += count;
                      output.add(new Tuple2<>(forward[0], count));
                  }
              } while (iterator.hasNext());
              if (sum > 0) output.add(new Tuple2<>(kv._1, sum));
              return output.iterator();
          }).reduceByKey((a, b) -> a + b);
    }

    /**
     * Intesect two sets of vertices to find the triangles.
     * Higher degree neighbors to a vertex, and the neighbors one step/edge further.
     * @param  hDegs   Higher degree neighbors.
     * @param  forward The neighbors one step/edge further.
     * @param  output  Number of triangles for a vertex.
     * @param  hIndex  Index to start iterating over hDegs.
     * @param  fIndex  Index to start iterating over forward.
     * @return  The overall number of neighbors.
     */
    private static long sortedIntersection(long[] hDegs, long[] forward, List<Tuple2<Long, Long> > output,
                                          long hIndex, long fIndex) {
        long fLen = forward.length;
        long hLen = hDegs.length;

        if (hDegs.length == 0 || fLen == 0)
            return 0;

        boolean leftRead = true;
        boolean rightRead = true;

        long h = 0;
        long f = 0;
        long count = 0;

        while (true) {

            if (hIndex >= hLen && fIndex >= fLen)
                break;

            if ((hIndex >= hLen && !rightRead) || (fIndex >= fLen && !leftRead))
                break;

            if (leftRead && hIndex < hLen) {
                h = hDegs[(int) hIndex++];
            }

            if (rightRead && fIndex < fLen) {
                f = forward[(int) fIndex++];
            }

            if (h == f) {
                if (output != null)
                    output.add(new Tuple2<Long, Long>(h, (long) 1));
                count++;
                leftRead = true;
                rightRead = true;
            } else if (h < f) {
                leftRead = true;
                rightRead = false;
            } else {
                leftRead = false;
                rightRead = true;
            }
        }
        return count;
    }

    /**
     * Compute global Clustering Coefficient based on Overall triangle count of
     * graph and the (Flitered Ordered) Neighbor List.
     * @param  overallTriangleCount of graph
     * @param  fonl                 FONL.
     * @return                      GLOBAL CLUSTERING COEFFICIENT.
     */
    public static float computeGCC(long overallTriangleCount, JavaPairRDD<Long, long[]> fonl) {
        long nodes = fonl.count();
        return overallTriangleCount / (float) (nodes * (nodes - 1));
    }

    /**
     * Compute the local clustering coefficient.
     * @param  vertexTC Vertices with their triangle counts.
     * @param  fonl     the FONL.
     * @return          List of Vertex with its Local Clustering Coefficient as Pair RDD.
     */
    public static JavaPairRDD<Long, Float> computeLcc(JavaPairRDD<Long, Long> vertexTC, JavaPairRDD<Long, long[]> fonl) {
        return fonl.leftOuterJoin(vertexTC)
                .mapValues(v -> (!v._2.isPresent() || v._1[0] < 2) ? 0.0f : 2.0f * v._2.get() / (v._1[0] * (v._1[0] - 1)));
      }

// Utilities ---------------------------------------------------------------------------------------------

    /**
     * Print global Clustering Coefficient to txt file.
     * @param  filePath the final Results Path.
     * @param  value     GLOBAL CLUSTERING COEFFICIENT value.
     */
    public static void printGlobalCCToFile(String filePath,String value)
            throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
        writer.write(value);

        writer.close();
    }
}
