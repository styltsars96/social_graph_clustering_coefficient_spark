package org.spark;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Dataset link: https://snap.stanford.edu/data/email-Enron.html
 */
public class Exercise {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: Exercise <inputpath> <outputpath>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf() /*.setMaster("local[4]")*/ .setAppName("Exercise");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load input file.
        JavaRDD<String> lines = sc.textFile(args[0]);

        // Do Processing to find the correlation coefficients.
        JavaPairRDD<Long, Float> result = Processing.compute(lines, sc, false);
        // Graph representation is of directed graph, but the actual graph in the dataset is undirected.
        // This means it does not need the (target, source tuple as it already exists)

        //After having found all the coefficients find the global.
        Broadcast<Float> globalResult = Processing.computeGlobal(sc);

        //Save Output of all the vertices' coefficients.
        result.saveAsTextFile(args[1]);

        //Send Global Correlation Coefficient to Standard Output as a message and save it as text File.
        String gCC = "RESULT: Global correlation coefficient:" + String.format("%.5f", globalResult.value());
        Processing.printGlobalCCToFile(args[1]+"/globalCorrelationCoefficient.txt",gCC);
        System.out.println(gCC);

        sc.stop();
        sc.close();

    }

}
