package cs523.StockSparkStream;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;


public class HBaseManager {
	private static final String TABLE_LIVE_NAME = "live_stock";
	private static final String TABLE_HISTORY_NAME = "history_stock";
	private static final String CF_DEFAULT = "stock_details";
	
	private Table liveTable;
	private Table historyTable;

	public HBaseManager(){}
	public boolean Init() throws IOException{
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum",Global.HbaseZookeeperQuorum);
		config.set("hbase.zookeeper.property.clientPort", "2181");
	    try {
			HBaseAdmin.checkHBaseAvailable(config);
		} catch (ServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			System.out.print("Connect Hbase.... ");

			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_LIVE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));
			
			HTableDescriptor tableHis = new HTableDescriptor(TableName.valueOf(TABLE_HISTORY_NAME));
			tableHis.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));


			if (!admin.tableExists(table.getTableName()))
			{
				System.out.print("Creating table live.... ");
				admin.createTable(table);
			}
			if (!admin.tableExists(tableHis.getTableName()))
			{
				System.out.print("Creating table history.... ");
				admin.createTable(tableHis);
			}
			
			System.out.println("Done!");	
			return true;
	        
		}catch(IOException ex){
			return false;
		}
	}
	
	public boolean AddNewRecord(String column, String value) throws IOException{
		try{
		Configuration config = HBaseConfiguration.create();
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			String[] columns = column.split(",");
			String[] values = value.split(Global.LinePattern, -1);
			
			liveTable = connection.getTable(TableName.valueOf(Bytes.toBytes(TABLE_LIVE_NAME)));
			historyTable = connection.getTable(TableName.valueOf(Bytes.toBytes(TABLE_HISTORY_NAME)));
			List<Put> puts = new ArrayList<Put>();
			List<Put> putsHis = new ArrayList<Put>();
			//symbol as key
			String key = "";
			int index = Arrays.asList(columns).indexOf("symbol");
			if(index > -1){
				key = Utilities.removeDoubleQuote(values[index]);
				Put put = new Put(Bytes.toBytes(key));
				Timestamp timestamp = new Timestamp(System.currentTimeMillis());
				Put put1 = new Put(Bytes.toBytes(key + timestamp.toString()));
				for(int i=0; i < columns.length; i++){
					String col = columns[i];
					String val = Utilities.removeDoubleQuote(values[i]);
					if(i != index){
						put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(col), Bytes.toBytes(val));
						puts.add(put);
					}
					put1.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(col), Bytes.toBytes(val));
					putsHis.add(put1);
				}
			
				liveTable.put(puts);
				historyTable.put(putsHis);
				return true;
			}
			return false;
		}
		}catch(Exception ex){
			return false;
		}
	
	}
	
}
