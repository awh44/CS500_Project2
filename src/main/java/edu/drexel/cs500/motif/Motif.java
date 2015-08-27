package edu.drexel.cs500.motif;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 
 * @author Austin Herring, Sanjana Raj
 * Frequent preference motif mining with Apache Spark SQL.
 *
 */
public final class Motif {
    
    private static JavaSparkContext sparkContext;
    private static SQLContext sqlContext;
    
    /**
     * Set up Spark and SQL contexts.
     */
    private static void init (String master, int numReducers) {
		Logger.getRootLogger().setLevel(Level.OFF);

		SparkConf sparkConf = new SparkConf().setAppName("Motif")
		                                     .setMaster(master)
		                                     .set("spark.sql.shuffle.partitions", "" + numReducers);
	
		sparkContext = new JavaSparkContext(sparkConf);
		sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);
    }
    
    /**
     * 
     * @param inFileName
     * @return
     */
    private static DataFrame initPref (String inFileName) {
		// read in the transactions file
		JavaRDD<String> prefRDD = sparkContext.textFile(inFileName);
	
		// establish the schema: PREF (tid: string, item1: int, item2: int)
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("tid", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("item1", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("item2", DataTypes.IntegerType, true));
		StructType prefSchema = DataTypes.createStructType(fields);

		JavaRDD<Row> rowRDD = prefRDD.map(
			new Function<String, Row>() {
				static final long serialVersionUID = 42L;
				public Row call(String record) throws Exception {
					String[] fields = record.split("\t");
					return  RowFactory.create(fields[0], 
						Integer.parseInt(fields[1].trim()), 
						Integer.parseInt(fields[2].trim()));
				}
			});

		// create DataFrame from prefRDD, with the specified schema
		return sqlContext.createDataFrame(rowRDD, prefSchema);
    }
    
    private static void saveOutput (DataFrame df, String outDir, String outFile) throws IOException {
		File outF = new File(outDir);
		outF.mkdirs();
		BufferedWriter outFP = new BufferedWriter(new FileWriter(outDir + "/" + outFile));
            
		List<Row> rows = df.toJavaRDD().collect();
		for (Row r : rows) {
			outFP.write(r.toString() + "\n");
		}
        
		outFP.close();
    }

	private static DataFrame generateAllPreferences(DataFrame pref)
	{
		DataFrame allPreferences = pref.toDF("atid", "aitem1", "aitem2");
		DataFrame currentPreferences = pref.toDF("ctid", "citem1", "citem2");
		do
		{
			//Extend the previously generated set of preferences by one. In other words, if on the previous interation
			//the path lengths between all the pairs of had length n, generate all of the preferences where the path length
			//is n + 1
			DataFrame newPreferences = currentPreferences.join(pref, currentPreferences.col("ctid").equalTo(pref.col("tid")))
			                                             .where(currentPreferences.col("citem2").equalTo(pref.col("item1")))
			                                             .select("ctid", "citem1", "item2");
			allPreferences = allPreferences.unionAll(newPreferences);

			//On the next iteration, the current paths of length "n + 1" will then be the paths of length "n"
			currentPreferences = newPreferences.toDF("ctid", "citem1", "citem2");
		} while (currentPreferences.count() != 0);
		allPreferences = allPreferences.distinct();
		allPreferences.show();

		return allPreferences;
	}

	private static DataFrame generateFrequentVMotifs(DataFrame allPreferences1, DataFrame allPreferences2, long numTransactions, double thresh)
	{
		DataFrame allVMotifs = allPreferences1.join(allPreferences2, allPreferences1.col("a1tid").equalTo(allPreferences2.col("a2tid")))
		                                   .where(allPreferences1.col("a1item2").equalTo(allPreferences2.col("a2item2")))
		                                   .where(allPreferences1.col("a1item1").lt(allPreferences2.col("a2item1")))
		                                   .distinct()
		                                   .groupBy("a1item1", "a1item2", "a2item1", "a2item2")
		                                   .count()
		                                   .select("a1item1", "a1item2", "a2item1", "count");
		return allVMotifs.where(allVMotifs.col("count").divide(numTransactions).geq(thresh));
	}
    
    public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.err.println("Usage: Motif <inFile> <support> <outDir> <master> <numReducers>");
			System.exit(1);
		}

		String inFileName = args[0].trim();
		double thresh =  Double.parseDouble(args[1].trim());
		String outDirName = args[2].trim();
		String master = args[3].trim();
		int numReducers = Integer.parseInt(args[4].trim());

		Motif.init(master, numReducers);
		Logger.getRootLogger().setLevel(Level.OFF);
		
		DataFrame pref = Motif.initPref(inFileName).as("pref");
		
		// your code goes here, setting these DataFrames to null as a placeholder
		DataFrame allPreferences = generateAllPreferences(pref);
		DataFrame allPreferences1 = allPreferences.toDF("a1tid", "a1item1", "a1item2");
		DataFrame allPreferences2 = allPreferences.toDF("a2tid", "a2item1", "a2item2");
		
		long numTransactions = pref.select("tid").distinct().count();
/*
		DataFrame lMotifs = allPreferences.join(allPreferences2, allPreferences.col("atid").equalTo(allPreferences2.col("a2tid")))
		                                  .where(allPreferences.col("aitem2").equalTo(allPreferences.col("a2item1")));
*/
		DataFrame vMotifs = generateFrequentVMotifs(allPreferences1, allPreferences2, numTransactions, thresh);
/*
		DataFrame aMotifs = null;
*/
/*
		try {
			Motif.saveOutput(lMotifs, outDirName + "/" + thresh, "L");
		} catch (IOException ioe) {
			System.out.println("Cound not output L-Motifs " + ioe.toString());
		}
*/
		
		try {
			Motif.saveOutput(vMotifs, outDirName + "/" + thresh, "V");
		} catch (IOException ioe) {
			System.out.println("Cound not output V-Motifs " + ioe.toString());
		}
/*
		try {
			Motif.saveOutput(aMotifs, outDirName + "/" + thresh, "A");
		} catch (IOException ioe) {
			System.out.println("Cound not output A-Motifs " + ioe.toString());
		}
*/
		System.out.println("Done");
		sparkContext.stop();
	        
    }
}
