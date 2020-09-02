package org.ngseq.metagenomics;

import antlr.StringUtils;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

public class AssemblyOutput {

    public static void main(String[] args) throws IOException  {
        SparkConf conf = new SparkConf().setAppName("AssemblyOutput");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Options options = new Options();

        Option out = new Option("out", true, "output");
        Option folderIn = new Option("in", true, "");
        Option parts = new Option("partitions", true, "");



        options.addOption(out);
        options.addOption(folderIn);
        options.addOption(parts);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
        }


        String output = (cmd.hasOption("out") == true) ? cmd.getOptionValue("out") : null;

        String in = (cmd.hasOption("in") == true) ? cmd.getOptionValue("in") : null;
        String partitions = (cmd.hasOption("partitions") == true) ? cmd.getOptionValue("partitions") : null;



        JavaRDD<String> aggregateRDD = sc.textFile(in);

        JavaRDD<String> crdd = aggregateRDD.map(fasta->{


            String[] fseq = fasta.trim().split("\t");


            String id = fseq[0].split("_")[0];
            //Give unique id for sequence
            String seq_id = UUID.randomUUID().toString();
            String seq = fseq[1];
            int Ncount = seq.length() - seq.replace("N", "").length();

            double N_fraction = (double) Ncount / seq.length() * 100;

            if (N_fraction < 10) {
                return ">"+seq_id+"\n"+seq;
            } else {
                return "abadeli deli delasa";
            }


        }).filter(f -> f.trim().startsWith(">"));



        crdd.repartition(Integer.valueOf(partitions)).saveAsTextFile(output);
        sc.stop();
    }
}
