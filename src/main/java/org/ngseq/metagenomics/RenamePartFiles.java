package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;

public class RenamePartFiles {

    public static void  main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("sortRawBamTCGA");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Options options = new Options();

        Option in = new Option("in", true, "");


        options.addOption(in);


        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("spark-submit <spark specific args>", options, true);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            System.exit(1);
        }
        String inputPath = (cmd.hasOption("in") == true) ? cmd.getOptionValue("in") : null;

        FileSystem fs = FileSystem.get(new Configuration());

        FileStatus[] dr = fs.listStatus(new Path(inputPath));
        for (FileStatus dir : dr) {
            System.out.println("directory " + dir.toString());
            FileStatus[] caseNames = fs.listStatus(dir.getPath());
            for (FileStatus fw_rf: caseNames){

            FileStatus[] split = fs.listStatus(fw_rf.getPath());

                for (int i = 0; i < split.length; i++) {
                    String fn = split[i].getPath().getName();

                    if (!fn.equalsIgnoreCase("_SUCCESS")) {
                        String folder = fw_rf.getPath().toUri().getRawPath();
                        String fileName = folder.substring(folder.lastIndexOf("/") + 1) + ".fq";

                        Path srcPath = new Path(split[i].getPath().toUri().getRawPath());
                        String newPath = fw_rf.getPath().getParent().toUri().getRawPath() + "/" + fileName;
                        Path dstPath = new Path(newPath);

                        FileUtil.copy(fs, srcPath, fs, dstPath, true, new Configuration());
                        fs.delete(new Path(fw_rf.getPath().toUri().getRawPath()));
                    }
                }


            }
        }

        sc.stop();

    }
}
