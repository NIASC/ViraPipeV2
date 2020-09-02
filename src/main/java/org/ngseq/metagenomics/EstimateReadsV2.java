package org.ngseq.metagenomics;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.functions.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EstimateReadsV2 {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("EstimateReadsV2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        Options options = new Options();
        Option pathOpt = new Option("in", true, "Path to fastq file in hdfs.");    //gmOpt.setRequired(true);
        Option pathVir = new Option("pathVir", true, "Path to fastq file in hdfs.");    //gmOpt.setRequired(true);
        Option outOpt = new Option("out", true, "");
        Option outGroupped = new Option("outG", true, "");
        options.addOption(pathOpt);
        options.addOption(outOpt);
        options.addOption(pathVir);
        options.addOption(outGroupped);

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
        String outDir = (cmd.hasOption("out") == true) ? cmd.getOptionValue("out") : null;
        String outDirGrouped = (cmd.hasOption("outG") == true) ? cmd.getOptionValue("outG") : null;

        Dataset countedReads = registerCountedReads(input, sc, sqlContext);
        countedReads.registerTempTable("CountedReads");


        Dataset countedReads2 = countedReads.filter(countedReads.col("count").$greater(4));



        //Dataset filtered = combined.filter(combined.col("family").notEqual("null"));

        Dataset pivotNAs = countedReads2.groupBy("contig").pivot("case").agg(org.apache.spark.sql.functions.sum(countedReads2.col("count")).as("reads"));


        Dataset pivot = pivotNAs.na().fill(0);
        Dataset sorted = pivot.sort(pivot.col("contig"));

        sorted.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save(outDir);


        /*
        JavaRDD<String> combinedRDD = dfToTabDelimited(combined);
        JavaRDD<String> filteredRDD = combinedRDD.filter(x -> !x.contains("null"));
        filteredRDD.saveAsTextFile(outDir);
         */
        Dataset groupped = countedReads2.groupBy("contig").agg(
                org.apache.spark.sql.functions.sum(countedReads.col("count")).as("reads"),
                org.apache.spark.sql.functions.count("case").as("cases"));

          groupped.registerTempTable("final");
          dfToTabDelimited(groupped).coalesce(1).saveAsTextFile(outDirGrouped);
    }

    private static JavaRDD<String> dfToTabDelimited(Dataset<Row> df) {
        return df.toJavaRDD().map(row -> {
            //qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore

            String output = row.getAs("contig") + "\t" + row.getAs("reads") + "\t" + row.getAs("cases");

            return output;
        });
    }

    private static Dataset<Row> registerCountedReads (String input, JavaSparkContext sc, SQLContext sqlContext) {
        JavaRDD<String> contigs = sc.textFile(input);

        // The schema is encoded in a string
        String schemaString = "contig count case";
        // Generate the schema based on the string of schema

        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName: schemaString.split(" ")) {
                fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows.
        JavaRDD<Row> rowRDD = contigs.map(
                (Function<String, Row>) record -> {
                    String[] fields1 = record.split("\t");
                    return RowFactory.create(fields1[0], fields1[1].trim(), fields1[2]);
                });

        // Apply the schema to the RDD.
        Dataset data = sqlContext.createDataFrame(rowRDD, schema);
        return data;

    }

    private static Dataset<Row> registerBlastViruses (String input, JavaSparkContext sc, SQLContext sqlContext) {
        JavaRDD<String> contigs = sc.textFile(input);

        // The schema is encoded in a string
        String schemaString = "contig acc identity coverage length e-value taxa";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName: schemaString.split(" ")) {
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = contigs.map(
                (Function<String, Row>) record -> {
                    String[] fields1 = record.split("\\|");
                    return RowFactory.create(fields1[0], fields1[1], fields1[2], fields1[3], fields1[4], fields1[5], fields1[6]);
                });

        // Apply the schema to the RDD.
        Dataset data = sqlContext.createDataFrame(rowRDD, schema);
        return data;

    }

}