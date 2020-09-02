package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by davbzh on 2017-04-14.
 */
public class SplitFasta {

    public static void main(String[] args) throws IOException {
        Options options = new Options();
        Option pathOpt = new Option( "in", true, "Path to fastq file in hdfs." );
        Option opOpt = new Option( "out", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );
        Option opPart = new Option( "partitions", "Divide or merge to n partitions" );

        options.addOption( opPart);
        options.addOption( pathOpt );
        options.addOption( opOpt );

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            // parse the command line arguments
            cmd = parser.parse( options, args );

        }
        catch( ParseException exp ) {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }

        String out = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
        String in = (cmd.hasOption("in")==true)? cmd.getOptionValue("in"):null;
        int partitions = (cmd.hasOption("partitions")==true)? Integer.valueOf(cmd.getOptionValue("partitions")):1000;

        SparkConf conf = new SparkConf().setAppName("SplitFasta");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.hadoopConfiguration().set("textinputformat.record.delimiter", ">");

        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] dirs = fs.listStatus(new Path(in));
        for (FileStatus dir : dirs) {
            String current = dir.getPath().toUri().getRawPath();
            JavaRDD<String> rdd = sc.textFile(current);
            JavaRDD<String> crdd = rdd.map(v -> ">" + v.trim()).repartition(partitions);

            String dr = dir.getPath().toUri().getRawPath();
            List<String> items = Arrays.asList(dr.split("\\s*/\\s*"));

            String name = items.get(items.size() - 1);

            crdd.saveAsTextFile(out + "/" + name );

        }
        sc.stop();
    }
}
