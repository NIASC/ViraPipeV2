package org.ngseq.metagenomics;

import htsjdk.samtools.SAMRecord;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.seqdoop.hadoop_bam.*;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import trimmomatic.*;

import static org.apache.spark.sql.functions.count;


public class QualityCheck {

  public static void main(String[] args) throws IOException {
      SparkConf conf = new SparkConf().setAppName("QualityCheck");

      JavaSparkContext sc = new JavaSparkContext(conf);
      SQLContext sqlContext = new SQLContext(sc);

      Options options = new Options();

      Option inOpt = new Option("in", true, " To read data from HDFS subdirectories");
      Option outOpt = new Option("out", true, "HDFS path for output files. If not present, the output files are not moved to HDFS.");
      Option phredOpt = new Option("phred", true, "phred value for quality check: default 33");
      Option leadOpt = new Option("leading", true, "value for leading trimming: default 3");
      Option trailOpt = new Option("trailing", true, "value for trailing trimming: default 3");
      Option windowlenOpt = new Option("slidingWindow", true, "length of sliding window: default 4");
      Option minLengthOpt = new Option("minlen", true, "drop the read if it is shorter than this value : default 18");
      Option adaptorsOpt = new Option("adaptors", true, "file in hdfs containing adaptor sequences");
      Option formatOpt = new Option("format", true, "output file format: default fastq");

      //
      Option cases = new Option("cases", true, "patient files");


      options.addOption(inOpt);
      options.addOption(outOpt);
      options.addOption(phredOpt);
      options.addOption(leadOpt);
      options.addOption(trailOpt);
      options.addOption(windowlenOpt);
      options.addOption(minLengthOpt);
      options.addOption(adaptorsOpt);
      options.addOption(formatOpt);
      //
      options.addOption(cases);


      CommandLineParser parser = new BasicParser();
      CommandLine cmd = null;

      try {
          // parse the command line arguments
          cmd = parser.parse(options, args);

      } catch (ParseException exp) {
          // oops, something went wrong
          System.err.println("Parsing failed.  Reason: " + exp.getMessage());
      }

      String in = (cmd.hasOption("in") == true) ? cmd.getOptionValue("in") : null;
      String outDir = (cmd.hasOption("out") == true) ? cmd.getOptionValue("out") : null;
      int phred = (cmd.hasOption("phred") == true) ? Integer.valueOf(cmd.getOptionValue("phred")) : 33;
      int leading = (cmd.hasOption("leading") == true) ? Integer.valueOf(cmd.getOptionValue("leading")) : 3;
      int trailing = (cmd.hasOption("trailing") == true) ? Integer.valueOf(cmd.getOptionValue("trailing")) : 3;
      int slidingWindow = (cmd.hasOption("slidingWindow") == true) ? Integer.valueOf(cmd.getOptionValue("slidingWindow")) : 4;
      int minlen = (cmd.hasOption("minlen") == true) ? Integer.valueOf(cmd.getOptionValue("minlen")) : 18;
      String adatptors = (cmd.hasOption("adaptors") == true) ? cmd.getOptionValue("adaptors") : null;
      String format = (cmd.hasOption("format") == true) ? cmd.getOptionValue("format") : null;
      String patients = (cmd.hasOption("cases") == true) ? cmd.getOptionValue("cases") : null;


      // int minc = (cmd.hasOption("minc")==true)? Integer.parseInt(cmd.getOptionValue("minc")):0;


      FileSystem fs = FileSystem.get(new Configuration());

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


      FileStatus[] dirs = fs.listStatus(new Path(in));


      for (FileStatus dir : dirs) {


          String fl = dir.getPath().toUri().getRawPath();
          List<String> items = Arrays.asList(fl.split("\\s*/\\s*"));
          String id = items.get(items.size() - 1).split("\\.")[0];
          if (lines.contains(id)) {
              FileStatus[] patient = fs.listStatus(dir.getPath());

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

                      Dataset<Row> sortedPairDF = pairDF.sort("readName", "secondOfPairFlag");

                      JavaPairRDD<Text, SequencedFragment> fqRDD = dfToFastqRDD(sortedPairDF);

                      JavaPairRDD<Text, SequencedFragment> qualityReadsRDD = fqRDD.mapPartitionsToPair(part -> {
                          List<Tuple2<Text, SequencedFragment>> QR = new ArrayList<>();
                          Logger logger = new Logger(true, false, false);
                          IlluminaClippingTrimmer ict1 = IlluminaClippingTrimmer.makeIlluminaClippingTrimmer(logger, adatptors, 2, 30, 10);
                          FastqRecord trailedTrimmed1;
                          FastqRecord trailedTrimmed2;
                          FastqRecord windowTrimmed1;
                          FastqRecord windowTrimmed2;
                          FastqRecord leadTrimmed1;
                          FastqRecord leadTrimmed2;
                          FastqRecord[] fastqR = new FastqRecord[2];
                          FastqRecord[] fastqRClipped;


                          while (part.hasNext()) {
                              Tuple2<Text, SequencedFragment> read1 = part.next();
                              String name = read1._1.toString().split("/")[0];
                              int key = Integer.parseInt(read1._1.toString().split("/")[1]);
                              String seq = read1._2.getSequence().toString();

                              String quality = read1._2.getQuality().toString();
                              fastqR[0] = new FastqRecord(name, seq, quality, phred);


                              if (part.hasNext()) {
                                  Tuple2<Text, SequencedFragment> read2 = part.next();
                                  String name2 = read2._1.toString().split("/")[0];
                                  int key2 = Integer.parseInt(read2._1.toString().split("/")[1]);
                                  String seq2 = read2._2.getSequence().toString();
                                  String quality2 = read2._2.getQuality().toString();

                                  if (name.equals(name2) && key2 == 2) {

                                      fastqR[1] = new FastqRecord(name2, seq2, quality2, phred);
                                      // trim Illumina adaptors
                                      fastqRClipped = ict1.processRecords(fastqR);

                                      if (fastqRClipped[0] != null && fastqRClipped[1] != null) {

                                          leadTrimmed1 = new LeadingTrimmer(leading).processRecord(fastqRClipped[0]);
                                          leadTrimmed2 = new LeadingTrimmer(leading).processRecord(fastqRClipped[1]);

                                      } else {
                                          continue;
                                      }
                                      if (leadTrimmed1 != null && leadTrimmed2 != null) {

                                          trailedTrimmed1 = new TrailingTrimmer(trailing).processRecord(leadTrimmed1);
                                          trailedTrimmed2 = new TrailingTrimmer(trailing).processRecord(leadTrimmed2);

                                      } else {
                                          continue;
                                      }
                                      if (trailedTrimmed1 != null && trailedTrimmed2 != null) {

                                          windowTrimmed1 = new SlidingWindowTrimmer(slidingWindow, 15).processRecord(trailedTrimmed1);
                                          windowTrimmed2 = new SlidingWindowTrimmer(slidingWindow, 15).processRecord(trailedTrimmed2);


                                      } else {
                                          continue;
                                      }


                                      if (windowTrimmed1 != null && windowTrimmed1.getSequence().length() >= minlen && windowTrimmed2 != null && windowTrimmed2.getSequence().length() >= minlen) {

                                          Text t1 = new Text(windowTrimmed1.getName() + "/1");
                                          SequencedFragment sf1 = new SequencedFragment();
                                          sf1.setSequence(new Text(windowTrimmed1.getSequence()));
                                          sf1.setQuality(new Text(windowTrimmed1.getQuality()));

                                          QR.add(new Tuple2<>(t1, sf1));

                                          Text t2 = new Text(windowTrimmed2.getName() + "/2");
                                          SequencedFragment sf2 = new SequencedFragment();
                                          sf2.setSequence(new Text(windowTrimmed2.getSequence()));
                                          sf2.setQuality(new Text(windowTrimmed2.getQuality()));

                                          QR.add(new Tuple2<>(t2, sf2));

                                      }

                                  } else {
                                      System.out.println("Single read: " + name);

                                  }

                              }

                          }

                          return QR.iterator();
                      });



                      JavaPairRDD<Text, SequencedFragment> fwd = qualityReadsRDD.filter(r -> r._1.toString().contains(" 1:N:0:1") || r._1.toString().contains("/1"));
                      JavaPairRDD<Text, SequencedFragment> rev = qualityReadsRDD.filter(r -> r._1.toString().contains(" 2:N:0:1") || r._1.toString().contains("/2"));
                      fwd.saveAsNewAPIHadoopFile(outDir + "/" + id + "/fastqforward", Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
                      rev.saveAsNewAPIHadoopFile(outDir + "/" + id + "/fastqreverse", Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());


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