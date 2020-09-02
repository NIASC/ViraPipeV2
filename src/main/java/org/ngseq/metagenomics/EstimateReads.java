package org.ngseq.metagenomics;
import com.github.lindenb.jbwa.jni.BwaIndex;
import com.github.lindenb.jbwa.jni.BwaMem;
import com.github.lindenb.jbwa.jni.ShortRead;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import org.stringtemplate.v4.ST;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class EstimateReads {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("EstimateReads");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Options options = new Options();
        Option pathOpt = new Option("in", true, "Path to fastq file in hdfs.");    //gmOpt.setRequired(true);
        Option refOpt = new Option("ref", true, "Path to fasta reference in local FS. (index must be available on every node under the same path)");
        Option fqoutOpt = new Option("out", true, "");
        Option caseOpt = new Option("cases", true, "");

        options.addOption(pathOpt);
        options.addOption(refOpt);
        options.addOption(fqoutOpt);
        options.addOption(caseOpt);


        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("spark-submit <spark specific args>", options, true);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
        }
        String input = cmd.getOptionValue("in");
        String ref = (cmd.hasOption("ref") == true) ? cmd.getOptionValue("ref") : null;
        String outDir = (cmd.hasOption("out") == true) ? cmd.getOptionValue("out") : null;
        String patients = cmd.getOptionValue("cases");

        // samtools flags
        ArrayList<Integer> flags = new ArrayList<Integer>(){{add(73);add(133);add(89);add(121);add(165);add(181);add(101);add(117);add(153);add(185);add(69);add(137);add(77);add(141);}};

        Broadcast<ArrayList<Integer>> broadcastedFlags = sc.broadcast(flags);


        FileSystem fs = FileSystem.get(new Configuration());

        Path patinentFile = new Path(patients);
        FSDataInputStream inputStream = fs.open(patinentFile);
        //Classical input stream usage
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String row;
        ArrayList<String> rows = new ArrayList<String>();
        while ((row = br.readLine()) != null) {
            String caseId = row.split("\t")[0];
            rows.add(caseId);
        }
        br.close();

        FileStatus[] fastq = fs.listStatus(new Path(input));



        for (FileStatus file : fastq) {
            String file_path = file.getPath().toUri().getRawPath();
            List<String> items = Arrays.asList(file_path.split("\\s*/\\s*"));
            String name = items.get(items.size() - 1).split("\\.")[0];
            if (rows.contains(name)) {
                System.out.println("input file " + file_path);
                JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(file_path, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());

                JavaRDD<String> alignmentRDD = fastqRDD.mapPartitions(split -> {
                    try {
                        System.load("/lib/native/libbwajni.so");
                    } catch (UnsatisfiedLinkError e) {
                        System.err.println("Native code library failed to load.\n" + e);
                        System.exit(1);
                    }

                    BwaIndex index = new BwaIndex(new File(ref));
                    BwaMem mem = new BwaMem(index);


                    List<ShortRead> L1 = new ArrayList<ShortRead>();
                    List<ShortRead> L2 = new ArrayList<ShortRead>();


                    while (split.hasNext()) {
                        Tuple2<Text, SequencedFragment> next = split.next();
                        String key = next._1.toString().split("/")[0];

                        SequencedFragment sf = new SequencedFragment();
                        sf.setQuality(new Text(next._2.getQuality().toString()));
                        sf.setSequence(new Text(next._2.getSequence().toString()));

                        if (split.hasNext()) {

                            Tuple2<Text, SequencedFragment> next2 = split.next();
                            String key2 = next2._1.toString().split("/")[0];

                            SequencedFragment sf2 = new SequencedFragment();
                            sf2.setQuality(new Text(next2._2.getQuality().toString()));
                            sf2.setSequence(new Text(next2._2.getSequence().toString()));

                            if (key.equalsIgnoreCase(key2)) {
                                L1.add(new ShortRead(key, sf.getSequence().toString().getBytes(), sf.getQuality().toString().getBytes()));
                                L2.add(new ShortRead(key2, sf2.getSequence().toString().getBytes(), sf2.getQuality().toString().getBytes()));
                            }
                        }
                    }
                    String[] aligns = mem.align(L1, L2);


                    if (aligns != null) {
                        ArrayList<String> filtered = new ArrayList<String>();

                        Arrays.asList(aligns).forEach(aln -> {
                            String[] fields = aln.split("\\t");

                            int flag = Integer.parseInt(fields[1]);


                            if (!broadcastedFlags.value().contains(flag)) {
                                String read_name = fields[0];
                                String contig = fields[2];
                                String bases = fields[9];
                                String quality = fields[10];

                                filtered.add(read_name + "\t" + bases + "\t" + quality);
                                //System.out.println(read_name + "\t" + flag + "\t" + contig + "\t" + name + "\t" + bases + "\t" + quality);

                                //filtered.add(contig);

                            }

                        });
                        return filtered.iterator();
                    } else
                        return new ArrayList<String>().iterator(); //NULL ALIGNMENTS


                });

                JavaPairRDD<String, Integer> counts = alignmentRDD.flatMap(x -> Arrays.asList(x).iterator())
                        .mapToPair(x -> new Tuple2<>(x, 1))
                        .reduceByKey((x, y) -> x + y);

                JavaRDD<String> countRdd = counts.map(x -> {
                    String contig = x._1;
                    String number = x._2.toString();
                    String line = contig + "\t" + number + "\t" + name;
                    return line;

                });


                JavaPairRDD<Text, SequencedFragment> finalFastqRDD = alignmentRDD.mapPartitionsToPair(record -> {

                    ArrayList<Tuple2<Text, SequencedFragment>> records = new ArrayList<Tuple2<Text, SequencedFragment>>();
                    while (record.hasNext()) {
                        String [] next = record.next().split("\t");
                        String key = next[0];
                        String sequence = next[1];
                        String quality = next[2];

                        Text t = new Text(key);
                        SequencedFragment sf = new SequencedFragment();
                        sf.setSequence(new Text(sequence));
                        sf.setQuality(new Text(quality));
                        records.add(new Tuple2<Text, SequencedFragment>(t, sf));
                    }

                        return records.iterator();

                });


               // System.out.println("output file " + outDir + "/" + name);

                //countRdd.coalesce(1).saveAsTextFile(outDir + "/" + name);
                if (finalFastqRDD.count() >4 ) {
                    finalFastqRDD.saveAsNewAPIHadoopFile(outDir + "/" + name, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
                }


            }

        }




        sc.stop();

        FileStatus[] dr = fs.listStatus(new Path(outDir));
        for (FileStatus dir : dr) {
            System.out.println("directory " + dir.toString());
            FileStatus[] folders = fs.listStatus(dir.getPath());
            for (int i = 0; i < folders.length; i++) {
                String fn = folders[i].getPath().getName();

                if (!fn.equalsIgnoreCase("_SUCCESS")) {
                    String folder = dir.getPath().toUri().getRawPath();
                    String fileName = folder.substring(folder.lastIndexOf("/") + 1) + ".fq";

                    Path srcPath = new Path(folders[i].getPath().toUri().getRawPath());
                    String newPath = dir.getPath().getParent().toUri().getRawPath() + "/" + fileName;
                    Path dstPath = new Path(newPath);

                    fs.rename(srcPath,dstPath);
                    fs.delete(new Path(dir.getPath().toUri().getRawPath()));


                }

            }
        }

    }


}
