package org.ngseq.metagenomics;

import htsjdk.samtools.SAMRecord;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.apache.spark.sql.functions.count;

public class SortUnalignedTCGA {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("SortUnalignedTCGA");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Options options = new Options();

        Option out = new Option("out", true, "output");
        Option folderIn = new Option("in", true, "");
        Option format = new Option("format", true, "");



        options.addOption(out);
        options.addOption(folderIn);
        options.addOption(format);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
        }


        String output = (cmd.hasOption("out") == true) ? cmd.getOptionValue("out") : null;

        String in = (cmd.hasOption("in") == true) ? cmd.getOptionValue("in") : null;
        String outFormat = (cmd.hasOption("format") == true) ? cmd.getOptionValue("format") : null;


        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] dirs = fs.listStatus(new Path(in));
        for (FileStatus dir : dirs) {
            System.out.println("directory " + dir.getPath().toUri().getRawPath());
            String current = dir.getPath().toUri().getRawPath();
            JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(current, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());
            JavaRDD<MyRead> rdd = fastqRDD.map(record -> {
                MyRead read = new MyRead();
                read.setKey(record._1.toString().split("/")[0]);
                read.setRead(Integer.parseInt(record._1.toString().split("/")[1]));
                read.setSequence(record._2.getSequence().toString());
                read.setQuality(record._2.getQuality().toString());


                return read;
            });

            Dataset df = sqlContext.createDataFrame(rdd, MyRead.class);
            df.registerTempTable("records");

            String tempName = String.valueOf((new Date()).getTime());

            // find pair ends
            Dataset pairEndKeys = df.groupBy("key").agg(count("*").as("count")).where("count == 2");

            Dataset<Row> pairDF = pairEndKeys.join(df, pairEndKeys.col("key").equalTo(df.col("key"))).drop(pairEndKeys.col("key"));

            Dataset<Row> sortedPairDF = pairDF.sort("key","read");
            // name of the case
            String dr = dir.getPath().toUri().getRawPath();
            List<String> items = Arrays.asList(dr.split("\\s*/\\s*"));

            String name = items.get(items.size() - 1);




            if (outFormat.equals("fq")){
                dfToFastqRDD(sortedPairDF).coalesce(1).saveAsNewAPIHadoopFile(output + "/" + name, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
            } else {
                // save normalized case
                dfToFasta(sortedPairDF).coalesce(1).saveAsTextFile(output + "/" + name);

            }

        }


        FileStatus[] dr = fs.listStatus(new Path(output));
        for (FileStatus dir : dr) {
          System.out.println("directory " + dir.toString());
          FileStatus[] files = fs.listStatus(dir.getPath());
              for (int i = 0; i < files.length; i++) {
                  String fn = files[i].getPath().getName();

                  if (!fn.equalsIgnoreCase("_SUCCESS")) {
                      String folder = dir.getPath().toUri().getRawPath();
                      String fileName = folder.substring(folder.lastIndexOf("/") + 1) + "." + outFormat;

                      Path srcPath = new Path(files[i].getPath().toUri().getRawPath());
                      String newPath = dir.getPath().getParent().toUri().getRawPath() + "/" + fileName;
                      Path dstPath = new Path(newPath);

                      FileUtil.copy(fs, srcPath, fs, dstPath, true, new Configuration());
                      fs.delete(new Path(dir.getPath().toUri().getRawPath()));
                  }

          }
        }
        sc.stop();
    }


    private static JavaRDD<String> dfToFasta(Dataset<Row> df) {

        return df.toJavaRDD().map(row -> {

            String name = row.getAs("key");

            if (row.getAs("read").equals(1)) {
                name = name + "/1";
            } else {
                name = name + "/2";
            }

            String sequence = row.getAs("sequence").toString();
            //TODO: check values
            String output = ">" + name + "\n" + sequence;

            return output;

        });

    }
    private static JavaPairRDD<Text, SequencedFragment> dfToFastqRDD(Dataset<Row> df) {
        return df.toJavaRDD().mapToPair(row ->  {
            String name = row.getAs("key");

            if (row.getAs("read").equals(1)) {
                name = name + "/1";
            } else {
                name = name + "/2";
            }
            Text t = new Text(name);
            SequencedFragment sf = new SequencedFragment();
            sf.setSequence(new Text((String) row.getAs("sequence")));
            sf.setQuality(new Text((String) row.getAs("quality")));

            return new Tuple2<Text, SequencedFragment>(t, sf);
        });
    }


}
