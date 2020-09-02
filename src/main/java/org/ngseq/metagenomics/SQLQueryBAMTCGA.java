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
import org.apache.hadoop.hdfs.BlockMissingException;
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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.count;




public class SQLQueryBAMTCGA {

  private static final Logger LOG = Logger.getLogger(SQLQueryBAMTCGA.class.getName());

  public static void main(String[] args) throws IOException {
      SparkConf conf = new SparkConf().setAppName("SQLQueryBAMTCGA");

      JavaSparkContext sc = new JavaSparkContext(conf);
      SQLContext sqlContext = new SQLContext(sc);

      Options options = new Options();

      Option out = new Option("out", true, "output");
      Option selection = new Option("selection", true, "HDFS path for output files. If not present, the output files are not moved to HDFS.");

      Option queryOpt = new Option("query", true, "SQL query string.");
      Option baminOpt = new Option("in", true, "");
      Option metaOUT = new Option("metaOUT", true, "");


      options.addOption(queryOpt);
      options.addOption(out);
      options.addOption(baminOpt);
      options.addOption(selection);
      options.addOption(metaOUT);
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
      String metaOut = (cmd.hasOption("metaOUT") == true) ? cmd.getOptionValue("metaOUT") : null;

      FileSystem fs = FileSystem.get(new Configuration());

      FileStatus[] dirs = fs.listStatus(new Path(in));
      for (FileStatus dir : dirs) {
          FileStatus[] files = fs.listStatus(dir.getPath());
          String bm;
          for (FileStatus f : Arrays.asList(files)) {

              if (f.getPath().getName().endsWith(".bam")) {
                  bm = f.getPath().toUri().getRawPath();

                      System.out.println("bam file" + bm);

                      JavaPairRDD<LongWritable, SAMRecordWritable> bamPairRDD = sc.newAPIHadoopFile(bm, AnySAMInputFormat.class, LongWritable.class, SAMRecordWritable.class, sc.hadoopConfiguration());
                      //Map to SAMRecord RDD
                     try {
                         JavaRDD<SAMRecord> samRDD = bamPairRDD.map(v1 -> v1._2().get());


                         JavaRDD<MyAlignment> rdd = samRDD.map(bam -> new MyAlignment(bam.getReadName(), bam.getStart(), bam.getReferenceName(), bam.getReadLength(), new String(bam.getReadBases(), StandardCharsets.UTF_8), bam.getCigarString(), bam.getReadUnmappedFlag(), bam.getDuplicateReadFlag(), bam.getBaseQualityString(), bam.getReadPairedFlag(), bam.getFirstOfPairFlag(), bam.getSecondOfPairFlag()));
                         //JavaRDD<MyAlignment> rdd = samRDD.map(bam -> new MyAlignment(bam.getReadName(), bam.getStart(), bam.getReferenceName(), bam.getReadLength(), new String(bam.getReadBases(), StandardCharsets.UTF_8), bam.getCigarString(), bam.getReadUnmappedFlag()));


                         Dataset<Row> samDF = sqlContext.createDataFrame(rdd, MyAlignment.class);
                         samDF.registerTempTable("records");

                         String unMapped = "SELECT * from records WHERE readUnmapped = TRUE";
                         String mappedToVirus = "SELECT * from records WHERE readUnmapped = FALSE AND referenceName NOT LIKE 'chr%'";
                         String filterHuman = "SELECT * from records WHERE readUnmapped = TRUE OR referenceName NOT LIKE 'chr%'";


                         //case name for writing files
                         String dr = dir.getPath().toUri().getRawPath();
                         List<String> items = Arrays.asList(dr.split("\\s*/\\s*"));

                         String name = items.get(items.size() - 1);


                         if (metaOut != null) {
                             Dataset df = sqlContext.sql(mappedToVirus);
                             Dataset meta = df.groupBy("referenceName").count();
                             JavaRDD<String> metaRDD = dfToMeta(meta);


                             metaRDD.coalesce(1).saveAsTextFile(metaOut + "/" + name);
                         }


                         if (select.equals("aligned")) {
                             Dataset df1 = sqlContext.sql(mappedToVirus);
                             JavaRDD<String> aligned = dfToRDD(df1);

                             aligned.saveAsTextFile(output + "/" + name);

                        /*
                          Dataset pairEndKeys = df2.groupBy("readName").agg(count("*").as("count")).where("count > 1");

                          Dataset<Row> pairDF = pairEndKeys.join(df2, pairEndKeys.col("readName").equalTo(samDF.col("readName"))).drop(pairEndKeys.col("readName"));
                          Long unmmaped_paired = pairDF.count();
                          System.out.println("total number of unligned paired rows " + unmmaped_paired.toString());

                          pairDF.registerTempTable("paired");
                          String forward = "SELECT * from paired WHERE firstOfPairFlag = TRUE";
                          String reverse = "SELECT * from paired WHERE firstOfPairFlag = FALSE";
                          Dataset<Row> forwardDF = sqlContext.sql(forward).sort("readName");
                          Dataset<Row> reverseDF = sqlContext.sql(reverse).sort("readName");

                          JavaPairRDD<Text, SequencedFragment> forwardRDD = dfToFastq(forwardDF);
                          JavaPairRDD<Text, SequencedFragment> reverseRDD = dfToFastq(reverseDF);
                          forwardRDD.coalesce(1).saveAsNewAPIHadoopFile(output + "/" + name + "/" + "forward", Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
                          reverseRDD.coalesce(1).saveAsNewAPIHadoopFile(output + "/" + name + "/" + "reverse", Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
                          */

                         } else if (select.equals("filterHuman")) {
                             Dataset df2 = sqlContext.sql(filterHuman);
                             JavaPairRDD<Text, SequencedFragment> FilteredHuman = dfToFastq(df2).repartition(100);
                             FilteredHuman.saveAsNewAPIHadoopFile(output + "/" + name, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());


                         } else {
                             Dataset df3 = sqlContext.sql(unMapped);
                             JavaPairRDD<Text, SequencedFragment> unAligned = dfToFastq(df3);
                             unAligned.saveAsNewAPIHadoopFile(output + "/" + name, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());


                         }
                     } catch (Exception e) {
                         System.out.println("Skipping this bam: " + bm);
                     }

              }
          }
      }

    sc.stop();

  /* FileStatus[] dr = fs.listStatus(new Path(output));
    for (FileStatus dir : dr) {
      System.out.println("directory " + dir.toString());
      FileStatus[] files = fs.listStatus(dir.getPath());

      for (FileStatus f : files) {
          FileStatus[] forward_reverse = fs.listStatus(f.getPath());

          for (int i = 0; i < forward_reverse.length; i++) {
              String fn = forward_reverse[i].getPath().getName();
              System.out.println("this is fn " + fn);

              if (!fn.equalsIgnoreCase("_SUCCESS")) {
                  String folder = f.getPath().toUri().getRawPath();
                  System.out.println("folder " + folder);
                  String fileName = folder.substring(folder.lastIndexOf("/") + 1) + ".fq";


                  Path srcPath = new Path(forward_reverse[i].getPath().toUri().getRawPath());
                  String newPath = f.getPath().getParent().toUri().getRawPath() + "/" + fileName;
                  System.out.println("this is new path " + newPath);
                  Path dstPath = new Path(newPath);


                  FileUtil.copy(fs, srcPath, fs, dstPath, true, new Configuration());
                  fs.delete(new Path(f.getPath().toUri().getRawPath()));
                  System.out.println("*" + files[i].getPath().toUri().getRawPath());
              }
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

    private static JavaRDD<String> dfToMeta (Dataset<Row> df) {
        return df.toJavaRDD().map(row ->  {

            String output = row.getAs("referenceName")+"\t"+row.getAs("count");

            return output;
        });
    }


  private static JavaPairRDD<Text, SequencedFragment> dfToFastq(Dataset<Row> df) {

    return df.toJavaRDD().mapToPair(row -> {



      String name = row.getAs("readName");
        if(row.getAs("readPairedFlag")){
            if(row.getAs("firstOfPairFlag"))
                name = name+"/1";
            if(row.getAs("secondOfPairFlag"))
                name = name+"/2";
        }

      //TODO: check values
      Text t = new Text(name);
      SequencedFragment sf = new SequencedFragment();
      sf.setSequence(new Text(row.getAs("bases").toString()));
      sf.setQuality(new Text(row.getAs("qualityBase").toString()));

      return new Tuple2<Text, SequencedFragment>(t, sf);
    });

  }


}