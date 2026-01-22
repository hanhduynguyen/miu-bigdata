package cs523.StockSparkStream;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
public class KafkaSparkStream {


	static HBaseManager hbaseManager = new HBaseManager();
	public static void main(String[] args) throws IOException, InterruptedException {
		//final String checkpointDir = args[0];
		final String kafkazkhost = args[0];
		final String hbasezkhost = args[1];
		final String topic = args[2];
		Global.HbaseZookeeperQuorum = hbasezkhost;
		Global.KafkaZookeeperQuorum = kafkazkhost;
		String zkQuorum = kafkazkhost + ":2181";//"10.10.14.117:2181";
		
		if(!hbaseManager.Init()) {
			System.out.println("Fail to connect HBase server....!");
			return;
		}
		SparkConf sparkConf = new SparkConf().setAppName("StockStream").setMaster("local[2]");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,Durations.milliseconds(1000));
		jssc.sparkContext().setLogLevel("ERROR");
		
		JavaSparkContext ctx = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));

        SQLContext sqlContext = new SQLContext(ctx);
		//jssc.checkpoint(checkpointDir);
		
	    Map<String, Integer> topics1 = new HashMap<String, Integer>();
	    topics1.put(topic, 1);
	    
		JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(jssc, zkQuorum, "stock_group",topics1);

		kafkaStream.foreachRDD(rdd->{
		      // Convert RDD[String] to RDD[case class] to DataFrame
		      JavaRDD<StockRecord> rowRDD = rdd.map(r->{
		    	  String cols = r._1();
		    	  String vals = r._2();
		    	  return StockRecord.getStockRecord(cols, vals);
		      });
		      DataFrame df1 = sqlContext.createDataFrame(rowRDD, StockRecord.class);

		      // Creates a temporary view using the DataFrame
		      df1.registerTempTable("livestock");
//
//		      
		      DataFrame df = sqlContext.sql("select symbol,closed,open,market from livestock");
		      df.show();
		
		      rdd.foreach(l->{
				if(l._1() != null && l._2() != null){
					//System.out.print(l._1());
					//System.out.print(l._2());
					hbaseManager.AddNewRecord(l._1(), l._2());
				}
			});
		      
		});
		//kafkaStream.print();
	
        
        jssc.start();
        jssc.awaitTermination();
	}


}
