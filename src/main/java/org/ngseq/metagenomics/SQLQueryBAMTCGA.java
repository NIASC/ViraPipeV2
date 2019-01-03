package org.ngseq.metagenomics;

import com.github.lindenb.jbwa.jni.ShortRead;
import htsjdk.samtools.SAMRecord;
import io.hops.VirapipeHopsPipeline;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.seqdoop.hadoop_bam.*;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.count;


/**
 * Usage
 spark-submit --master local[40] --driver-memory 4g --executor-memory 4g --class org.ngseq.metagenomics.SQLQueryBAM target/metagenomics-0.9-jar-with-dependencies.jar -in aligned -out unmapped -query "SELECT * from records WHERE readUnmapped = TRUE"

 **/


public class SQLQueryBAMTCGA {

  private static final Logger LOG = Logger.getLogger(SQLQueryBAMTCGA.class.getName());

  public static void main(String[] args) throws IOException {
      SparkConf conf = new SparkConf().setAppName("SQLQueryBAMTCGA");

      JavaSparkContext sc = new JavaSparkContext(conf);
      SQLContext sqlContext = new SQLContext(sc);

      Options options = new Options();

      Option out = new Option("out", true, "output");
      Option virOut = new Option("virDir", true, "HDFS path for output files. If not present, the output files are not moved to HDFS.");
      Option selection = new Option("selection", true, "HDFS path for output files. If not present, the output files are not moved to HDFS.");

      Option queryOpt = new Option("query", true, "SQL query string.");
      Option baminOpt = new Option("in", true, "");

      options.addOption(queryOpt);
      options.addOption(out);
      options.addOption(baminOpt);
      options.addOption(virOut);
      options.addOption(selection);
      CommandLineParser parser = new BasicParser();
      CommandLine cmd = null;
      try {
          cmd = parser.parse(options, args);

      } catch (ParseException exp) {
          System.err.println("Parsing failed.  Reason: " + exp.getMessage());
      }


      String output = (cmd.hasOption("out") == true) ? cmd.getOptionValue("out") : null;

      String in = (cmd.hasOption("in") == true) ? cmd.getOptionValue("in") : null;
      String select = (cmd.hasOption("selection") == true) ? cmd.getOptionValue("selection") : null;

      FileSystem fs = FileSystem.get(new Configuration());

      ArrayList<String> bamToFastaq = new ArrayList<>();
      FileStatus[] dirs = fs.listStatus(new Path(in));
      for (FileStatus dir : dirs) {
          System.out.println("directory " + dir.toString());
          FileStatus[] files = fs.listStatus(dir.getPath());
          String bm;
          for (FileStatus f : Arrays.asList(files)) {

              if (f.getPath().getName().endsWith(".bam")) {
                  bm = f.getPath().toUri().getRawPath();
                  JavaPairRDD<LongWritable, SAMRecordWritable> bamPairRDD = sc.newAPIHadoopFile(bm, AnySAMInputFormat.class, LongWritable.class, SAMRecordWritable.class, sc.hadoopConfiguration());
              //Map to SAMRecord RDD
              JavaRDD<SAMRecord> samRDD = bamPairRDD.map(v1 -> v1._2().get());
              JavaRDD<MyAlignment> rdd = samRDD.map(bam -> new MyAlignment(bam.getReadName(), bam.getStart(), bam.getReferenceName(), bam.getReadLength(), new String(bam.getReadBases(), StandardCharsets.UTF_8), bam.getCigarString(), bam.getReadUnmappedFlag(), bam.getDuplicateReadFlag(), bam.getBaseQualityString(), bam.getReadPairedFlag(), bam.getFirstOfPairFlag(), bam.getSecondOfPairFlag()));


              Dataset<Row> samDF = sqlContext.createDataFrame(rdd, MyAlignment.class);
              samDF.registerTempTable("records");
              Long total = samDF.count();
              System.out.println("totol number of rows " + total.toString());

              if (select.equals("all")) {

                  Dataset pairEndKeys = samDF.groupBy("readName").agg(count("*").as("count")).where("count > 1");
                  Dataset<Row> pairDF = pairEndKeys.join(samDF, pairEndKeys.col("readName").equalTo(samDF.col("readName"))).drop(pairEndKeys.col("readName"));
                  Long total_paired = pairDF.count();

                  pairDF.registerTempTable("paired");
                  System.out.println("totol number of paired rows " + total_paired.toString());
                  String forward = "SELECT * from paired WHERE firstOfPairFlag = TRUE";
                  String reverse = "SELECT * from paired WHERE firstOfPairFlag = FALSE";
                  Dataset<Row> forwardDF = sqlContext.sql(forward).sort("readName");
                  Dataset<Row> reverseDF = sqlContext.sql(reverse).sort("readName");

                  JavaPairRDD<Text, SequencedFragment> forwardRDD = dfToFastq(forwardDF);
                  JavaPairRDD<Text, SequencedFragment> reverseRDD = dfToFastq(reverseDF);
                  forwardRDD.coalesce(1).saveAsNewAPIHadoopFile(output + "/" + "forward", Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
                  reverseRDD.coalesce(1).saveAsNewAPIHadoopFile(output + "/" + "reverse", Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());


              } else {
                  String unMapped = "SELECT * from records WHERE readUnmapped = TRUE";


                  List<String> items = Arrays.asList(bm.split("\\s*/\\s*"));
                  for (String item : items) {
                      LOG.log(Level.SEVERE, "here are the times" + item);
                  }

                  String name = items.get(items.size() - 1);


                  Dataset df2 = sqlContext.sql(unMapped);
                  df2.show();


                  //metadata.coalesce(1).write().csv(bwaOutDir + "/" + name);
                  //JavaRDD<String> tabDelRDD = dfToRDD(df2);

                  JavaPairRDD<Text, SequencedFragment> fastqRDD = dfToFastq(df2);
                  //tabDelRDD.saveAsTextFile(bwaOutDir+ "/" + name);
                  fastqRDD.coalesce(1).saveAsNewAPIHadoopFile(output + "/" + name, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
                  //fastqRDD1.coalesce(1).saveAsNewAPIHadoopFile(output+ "/" + name + "mapped", Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
              }
              }
          }
      }

    sc.stop();

   /*FileStatus[] dirs = fs.listStatus(new Path(output));
    for (FileStatus dir : dirs) {
      System.out.println("directory " + dir.toString());
      FileStatus[] files = fs.listStatus(dir.getPath());
      for (int i = 0; i < files.length; i++) {
        String fn = files[i].getPath().getName();


        if (!fn.equalsIgnoreCase("_SUCCESS")) {
          String folder = dir.getPath().toUri().getRawPath();
          System.out.println("folder " + folder);
          String fileName = folder.substring(folder.lastIndexOf("/")+1) + ".fq";



          Path srcPath = new Path(files[i].getPath().toUri().getRawPath());
          String newPath = dir.getPath().getParent().toUri().getRawPath() + "/" + fileName;
          System.out.println("this is new path " + newPath);
          Path dstPath = new Path(newPath);


          FileUtil.copy(fs, srcPath, fs, dstPath,true, new Configuration());
          fs.delete(new Path(dir.getPath().toUri().getRawPath()));
          System.out.println("*" + files[i].getPath().toUri().getRawPath());
        }
      }
    }*/

  }


  private static JavaRDD<String> dfToRDD (Dataset<Row> df) {
    return df.toJavaRDD().map(row ->  {

      String output = row.getAs("bases")+"\t"+row.getAs("cigar")+"\t"+row.getAs("duplicateRead")+"\t"
              +row.getAs("firstOfPairFlag")+"\t"+row.getAs("length")+"\t"+row.getAs("qualityBase")+"\t"+row.getAs("readName")+"\t"
              +row.getAs("readPairedFlag")+"\t"+row.getAs("readUnmapped") +"\t"+row.getAs("referenceName")+"\t"
              +row.getAs("secondOfPairFlag")+"\t"+row.getAs("start");

      return output;
    });
  }


  private static JavaPairRDD<Text, SequencedFragment> dfToFastq(Dataset<Row> df) {

    return df.toJavaRDD().mapToPair(row -> {



      String name = row.getAs("readName");


      //TODO: check values
      Text t = new Text(name);
      SequencedFragment sf = new SequencedFragment();
      sf.setSequence(new Text(row.getAs("bases").toString()));
      sf.setQuality(new Text(row.getAs("qualityBase").toString()));

      return new Tuple2<Text, SequencedFragment>(t, sf);
    });

  }


}