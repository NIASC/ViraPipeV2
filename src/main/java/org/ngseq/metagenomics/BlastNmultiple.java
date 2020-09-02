package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class BlastNmultiple {
    private static final Logger LOG = Logger.getLogger(BlastN.class.getName());

    public static void main(String[] args) throws IOException {

        Options options = new Options();
        options.addOption(new Option( "temp", "Temporary output"));
        options.addOption(new Option( "out", true, "" ));
        options.addOption(new Option( "in", true, "" ));

        options.addOption(new Option( "word_size", ""));
        options.addOption(new Option( "gapopen", true, "" ));
        options.addOption(new Option( "gapextend", true, "" ));
        options.addOption(new Option( "penalty", true, "" ));
        options.addOption(new Option( "reward", true, "" ));
        options.addOption(new Option( "max_target_seqs", true, "" ));
        options.addOption(new Option( "evalue", true, "" ));
        options.addOption(new Option( "show_gis", "" ));
        options.addOption(new Option( "outfmt", true, "" ));
        options.addOption(new Option( "db", true, "Path to local BlastNT database (database must be available on every node under the same path)" ));
        options.addOption(new Option( "task", true, "" ));
        options.addOption(new Option( "num_threads", true, "" ));
        options.addOption(new Option( "taxname", true, "Use Blast taxonomy names for filtering e.g. viruses, bacteria, archaea" ));
        options.addOption(  new Option( "bin", true,"Path to blastn binary, defaults calls 'blastn'" ) );

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "spark-submit <spark specific args>", options, true );

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse( options, args );
        }
        catch( ParseException exp ) {
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
            System.exit(1);
        }

        String input = cmd.getOptionValue("in");
        String output = cmd.getOptionValue("out");
        int word_size = (cmd.hasOption("word_size")==true)? Integer.valueOf(cmd.getOptionValue("word_size")):11;
        int gapopen = (cmd.hasOption("gapopen")==true)? Integer.valueOf(cmd.getOptionValue("gapopen")):0;
        int gapextend = (cmd.hasOption("gapextend")==true)? Integer.valueOf(cmd.getOptionValue("gapextend")):2;
        int penalty = (cmd.hasOption("penalty")==true)? Integer.valueOf(cmd.getOptionValue("penalty")):-1;
        int reward = (cmd.hasOption("reward")==true)? Integer.valueOf(cmd.getOptionValue("reward")):1;
        int max_target_seqs = (cmd.hasOption("max_target_seqs")==true)? Integer.valueOf(cmd.getOptionValue("max_target_seqs")):10;
        double evalue = (cmd.hasOption("evalue")==true)? Double.valueOf(cmd.getOptionValue("evalue")):0.001;
        boolean show_gis = cmd.hasOption("show_gis");
        String outfmt = (cmd.hasOption("outfmt")==true)? cmd.getOptionValue("outfmt"):"\"6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore qlen slen\"";
        String db = cmd.getOptionValue("db");
        String task = (cmd.hasOption("task")==true)? cmd.getOptionValue("task"):"blastn";
        int num_threads = (cmd.hasOption("num_threads")==true)? Integer.valueOf(cmd.getOptionValue("num_threads")):1;
        String taxname = (cmd.hasOption("taxname")==true)? cmd.getOptionValue("taxname"):"";
        String bin = (cmd.hasOption("bin")==true)? cmd.getOptionValue("bin"):"blastn";


        SparkConf conf = new SparkConf().setAppName("BlastN");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.hadoopConfiguration().set("textinputformat.record.delimiter", ">");
        FileSystem filesystem = FileSystem.get(new Configuration());
        FileStatus[] dirs = filesystem.listStatus(new Path(input));
        for (FileStatus dir : dirs) {
            String current = dir.getPath().toUri().getRawPath();

            FileStatus[] st = filesystem.listStatus(new Path(current));
            ArrayList<String> splitFileList = new ArrayList<>();
            for (int i = 0; i < st.length; i++) {
                if (!st[i].isDirectory()) {
                    if (st[i].getLen() > 1) {
                        splitFileList.add(st[i].getPath().toUri().getRawPath().toString());
                        System.out.println(st[i].getPath().toUri().getRawPath().toString());
                    }
                }
            }

            JavaRDD<String> fastaFilesRDD = sc.parallelize(splitFileList, splitFileList.size());
            Broadcast<String> bs = sc.broadcast(filesystem.getUri().toString());
            System.out.println(" varibale - broadcast bs: " + filesystem.getUri().toString());
            JavaRDD<String> outRDD = fastaFilesRDD.mapPartitions(f -> {
                Process process;
                String fname = f.next();

//	    Path srcInHdfs = new Path(fname);
//	    Path destInTmp = new Path("file:///tmp/" + srcInHdfs.getName());
//	    fs.copyToLocalFile(false, srcInHdfs, destInTmp);
            /*
            JavaRDD<String> srcInHdfs = sc.textFile(fname);
            String filename = fname.substring(fname.lastIndexOf("/"));
            srcInHdfs.saveAsTextFile("file:///tmp/" + filename); */


                DFSClient client = new DFSClient(URI.create(bs.getValue()), new Configuration());

                DFSInputStream hdfsstream = client.open(fname);
                String blastn_cmd;

//                blastn_cmd = "cat /tmp/"+srcInHdfs.getName()+" | blastn -db "+db+" -num_threads "+num_threads+" -task megablast -word_size "+word_size+" -max_target_seqs "+max_target_seqs+" -evalue "+evalue+" " + ((show_gis == true) ? "-show_gis " : "") + " -outfmt "+outfmt;
//            else
//                blastn_cmd = "cat /tmp/"+srcInHdfs.getName()+" | blastn -db "+db+" -num_threads "+num_threads+" -word_size "+word_size+" -gapopen "+gapopen+" -gapextend "+gapextend+" -penalty "+penalty+" -reward "+reward+" -max_target_seqs "+max_target_seqs+" -evalue "+evalue+" " + ((show_gis == true) ? "-show_gis " : "") + " -outfmt "+outfmt;

                if (task.equalsIgnoreCase("megablast"))
                    blastn_cmd = bin + " -db " + db + " -num_threads " + num_threads + " -task megablast -word_size " + word_size + " -max_target_seqs " + max_target_seqs + " -evalue " + evalue + " " + ((show_gis == true) ? "-show_gis " : "") + " -outfmt " + outfmt;
                else
                    blastn_cmd = bin + " -db " + db + " -num_threads " + num_threads + " -word_size " + word_size + " -gapopen " + gapopen + " -gapextend " + gapextend + " -penalty " + penalty + " -reward " + reward + " -max_target_seqs " + max_target_seqs + " -evalue " + evalue + " " + ((show_gis == true) ? "-show_gis " : "") + " -outfmt " + outfmt;

                System.out.println(blastn_cmd);

                ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", blastn_cmd);
                process = pb.start();

                BufferedReader hdfsinput = new BufferedReader(new InputStreamReader(hdfsstream));
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
                String line;
                while ((line = hdfsinput.readLine()) != null) {
                    writer.write(line);
                    writer.newLine();
                }
                writer.close();

                BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String bline;
                ArrayList<String> out = new ArrayList<String>();
                while ((bline = in.readLine()) != null) {
                    out.add(bline);
                }

            /*
            BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String e;
            while ((e = err.readLine()) != null) {
                out.add(e);
            }
            */
                in.close();
            /*
            File fLocal = new File("/tmp/"+filename);
            try {
                fLocal.delete();
            } catch (SecurityException ex) {
                System.err.println("Could not delete local file:/tmp/" + filename);
            } "*/

                return out.iterator();
            });

            String dr = dir.getPath().toUri().getRawPath();
            List<String> items = Arrays.asList(dr.split("\\s*/\\s*"));

            String name = items.get(items.size() - 1);

            if (taxname != "")
                outRDD.filter(res -> {
                    String[] fields = res.split("\t");
                    String taxonomy = fields[res.length()];
                    return taxonomy.equalsIgnoreCase(taxname);
                }).saveAsTextFile(output);
            else
                outRDD.saveAsTextFile(output + "/" + name);

        }

        sc.stop();
    }
}
