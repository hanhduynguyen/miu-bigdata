package cs523.StockSparkStream;

import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class StockQuery {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf sparkConf = new SparkConf().setAppName("StockStreaming").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        
//        SQLContext sqlContext = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate().sqlContext
//
//
//		Dataset<Row> ds = sqlContext.read().json(rdd);

//        HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);
//        Row[] results = hiveContext.sql("FROM im_view_live_stock SELECT *").collect();
//        DataFrame df =  sqlContext.sql("select * from im_live_stock");
//        df.show();
        String sqlMapping = "symbol STRING :key" + ", open STRING stock_details:regularMarketOpen" + 
        					",closed STRING stock_details:regularMarketPreviousClose" +
        					",price STRING stock_details:regularMarketPrice";

        HashMap<String, String> colMap = new HashMap<String, String>();
        colMap.put("hbase.columns.mapping", sqlMapping);
        colMap.put("hbase.table", "live_stock");
        colMap.put("hbase.spark.use.hbasecontext", "false");

        // DataFrame dfJail =
        DataFrame df = sqlContext.read().format("org.apache.hadoop.hbase.spark").options(colMap).load();
        //DataFrame df = sqlContext.load("org.apache.hadoop.hbase.spark", colMap);

        // This is useful when issuing SQL text queries directly against the
        // sqlContext object.
        df.registerTempTable("temp_emp");

        DataFrame result = sqlContext.sql("SELECT * from live_stock");
        System.out.println("df  " + df);
        System.out.println("result " + result);

        df.show();  
	}

}
