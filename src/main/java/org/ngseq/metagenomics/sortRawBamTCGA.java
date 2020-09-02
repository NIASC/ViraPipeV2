package org.ngseq.metagenomics;

import htsjdk.samtools.SAMRecord;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.apache.spark.sql.functions.count;

public class sortRawBamTCGA {

        public static void  main(String[] args) throws IOException {
            SparkConf conf = new SparkConf().setAppName("sortRawBamTCGA");
            JavaSparkContext sc = new JavaSparkContext(conf);

            SQLContext sqlContext = new SQLContext(sc);
            Options options = new Options();

            Option in = new Option("in", true, "");
            Option out = new Option("out", true, "");
            Option cases = new Option("cases", true, "");


            options.addOption(in);
            options.addOption(out);
            options.addOption(cases);

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
            String outDir = (cmd.hasOption("out") == true) ? cmd.getOptionValue("out") : null;
            String patients = cmd.getOptionValue("cases");

            FileSystem fs = FileSystem.get(new Configuration());


            /*
            Path patinentFile = new Path(patients);
            FSDataInputStream inputStream = fs.open(patinentFile);
            //Classical input stream usage
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            ArrayList<String> lines = new ArrayList<String>();
            while ((line = br.readLine()) != null) {
                String caseId = line.split("\t")[0];
                lines.add(caseId);
            }
            br.close();


            inputStream.close();
            //fs.close();


            */

            FileStatus[] files = fs.listStatus(new Path(inputPath));

            for (FileStatus file : files) {
                String fl = file.getPath().toUri().getRawPath();
                List<String> items = Arrays.asList(fl.split("\\s*/\\s*"));
                String name = items.get(items.size() - 1).split("\\.")[0];
                // if (lines.contains(name))
                //{

                FileStatus[] patient = fs.listStatus(file.getPath());

                for (FileStatus f : Arrays.asList(patient)) {
                    System.out.println("target patoient " + f.toString());
                    if (f.getPath().getName().endsWith(".bam")) {
                        String rawBam = f.getPath().toUri().getRawPath();
                        JavaPairRDD<LongWritable, SAMRecordWritable> bamPairRDD = sc.newAPIHadoopFile(rawBam, AnySAMInputFormat.class, LongWritable.class, SAMRecordWritable.class, sc.hadoopConfiguration());
                        //Map to SAMRecord RDD
                        JavaRDD<SAMRecord> samRDD = bamPairRDD.map(v1 -> v1._2().get());
                        JavaRDD<MyAlignment> rdd = samRDD.map(bam -> new MyAlignment(bam.getReadName(), bam.getStart(), bam.getReferenceName(), bam.getReadLength(), new String(bam.getReadBases(), StandardCharsets.UTF_8), bam.getCigarString(), bam.getReadUnmappedFlag(), bam.getDuplicateReadFlag(), bam.getBaseQualityString(), bam.getReadPairedFlag(), bam.getFirstOfPairFlag(), bam.getSecondOfPairFlag()));

                        Dataset<Row> samDF = sqlContext.createDataFrame(rdd, MyAlignment.class);
                        samDF.registerTempTable("records");


                        Dataset pairEndKeys = samDF.groupBy("readName").agg(count("*").as("count")).where("count == 2");

                        Dataset<Row> pairDF = pairEndKeys.join(samDF, pairEndKeys.col("readName").equalTo(samDF.col("readName"))).drop(pairEndKeys.col("readName"));


                        pairDF.registerTempTable("paired");
                        String forward = "SELECT * from paired WHERE firstOfPairFlag = TRUE";
                        String reverse = "SELECT * from paired WHERE firstOfPairFlag = FALSE";
                        Dataset<Row> forwardDF = sqlContext.sql(forward).sort("readName");
                        Dataset<Row> reverseDF = sqlContext.sql(reverse).sort("readName");

                        JavaPairRDD<Text, SequencedFragment> forwardRDD = dfToFastqRDD(forwardDF);
                        JavaPairRDD<Text, SequencedFragment> reverseRDD = dfToFastqRDD(reverseDF);
                        forwardRDD.coalesce(1).saveAsNewAPIHadoopFile(outDir + "/" + name + "/" + "forward", Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
                        reverseRDD.coalesce(1).saveAsNewAPIHadoopFile(outDir + "/" + name + "/" + "reverse", Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());


                        // Dataset<Row> sortedPairDF = pairDF.sort("readName","secondOfPairFlag");

                        //dfToFastqRDD(sortedPairDF).coalesce(1).saveAsNewAPIHadoopFile(outDir + "/" + name, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());

                    }
                }

                //}
            }




            FileStatus[] dr = fs.listStatus(new Path(outDir));
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



    private static JavaPairRDD<Text, SequencedFragment> dfToFastqRDD(Dataset<Row> df) {
        return df.toJavaRDD().mapToPair(row ->  {
            String name = row.getAs("readName");

            if (row.getAs("firstOfPairFlag")) {
                name = name + "/1";
            } else {
                name = name + "/2";
            }
            Text t = new Text(name);
            SequencedFragment sf = new SequencedFragment();
            sf.setSequence(new Text((String) row.getAs("bases")));
            sf.setQuality(new Text((String) row.getAs("qualityBase")));

            return new Tuple2<Text, SequencedFragment>(t, sf);
        });
    }


    }
