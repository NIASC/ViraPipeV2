package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.util.ArrayList;

import static org.ngseq.metagenomics.QualityCheckFastq.GetPathArray;

public class ConcatParts {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("ConcatParts");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Options options = new Options();

        Option input = new Option("in", true, "");

        options.addOption(input);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
        }


        FileSystem fs = FileSystem.get(new Configuration());

        String inDir = (cmd.hasOption("in") == true) ? cmd.getOptionValue("in") : null;

        FileStatus[] dr = fs.listStatus(new Path(inDir));
        for (FileStatus dir : dr) {

            FileStatus[] files = fs.listStatus(dir.getPath());

            ArrayList<Path> srcPath = new ArrayList<>();
            for (int i = 0; i < files.length; i++) {
                String fn = files[i].getPath().getName();

                if (!fn.equalsIgnoreCase("_SUCCESS")) {

                    srcPath.add(new Path(files[i].getPath().toUri().getRawPath()));

                }

            }
            String folder = dir.getPath().toUri().getRawPath();
            System.out.println("folder" + folder);
            String fileName = folder.substring(folder.lastIndexOf("/") + 1);
            System.out.println("fileName" + fileName);

            String newPath = dir.getPath().getParent().toUri().getRawPath() + "/" + fileName+ "/" + fileName + ".fq";
            System.out.println("newPath" + newPath);

            Path dstPath = new Path(newPath);
            System.out.println("dstPath" + dstPath);


            Path [] paths = GetPathArray(srcPath);
            fs.concat(dstPath, paths);
        }

        sc.stop();

    }



}
