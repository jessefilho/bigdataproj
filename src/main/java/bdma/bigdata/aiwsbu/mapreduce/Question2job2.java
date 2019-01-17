package bdma.bigdata.aiwsbu.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Question2job2 {
	public static class Mapper extends TableMapper <Text,  DoubleWritable>{
		 public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
				   throws IOException, InterruptedException {

				  try {
				 
				   // get rowKey and convert it to string
				   String xtr = Bytes.toString(rowKey.get());
				   System.out.println("###############");				   
				   String [] in = xtr.split("/");
				   String inKey = in[0];
				   // get year as output key  and convert it to string 
				   String year = in[1];				   
				   // get grades column in byte format first and then convert it to double
				   byte[] bGrades = columns.getValue(Bytes.toBytes("result"), Bytes.toBytes("average"));
				   double grades = ByteBuffer.wrap(bGrades).getDouble();
				   // set year as output key and average grade as output value
				   context.write(new Text(inKey+"/"+year),new  DoubleWritable(grades));

				  
				  } catch (RuntimeException e){
				   e.printStackTrace();
				  }
				 }
				}
public static class Reducer extends TableReducer <Text, DoubleWritable, ImmutableBytesWritable> {
		 
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		  try {
		   double rate;
		   double sum = 0;
		   double total =0;
		   double intGrades;
		   for (DoubleWritable grade: values) {
			// calculate total number of students for a given year
			total++;
		    intGrades = grade.get();
		    // calculate  number of students having average > 1000  for a given year
		    if (intGrades > 1000 ) {
		    sum += 1;	
		    }
		    
		   } 
		   //Calculate rate
		   rate=sum/total;
		   String Srate=String.format("%.2f", rate);
		   System.out.println("*** year" +" "+key+""+"*** rate " + Srate);
		   // create hbase put with rowkey as year
		   Put insHBase = new Put(key.getBytes());
		   // insert rate value to hbase 
		   insHBase.add(Bytes.toBytes("result"), Bytes.toBytes("rate"), Bytes.toBytes(Srate));
		   // write data to Hbase table
		   context.write(null, insHBase);

		  } catch (Exception e) {
		   e.printStackTrace();
		  }
		 }
		 
	}
public static  void main(String[] args) throws Exception {
	 Configuration conf = HBaseConfiguration.create();
	 // define scan 
	 Scan scan = new Scan();
	 Job job = Job.getInstance(conf,"Question2job2"); 
	 TableName tableName = TableName.valueOf("A_21805893:Q2job1");
	 Connection connection =ConnectionFactory.createConnection(conf);
	 Table table =connection.getTable(tableName);
	 job.setJarByClass(Question2job2.class);
	 //define input hbase table
	    TableMapReduceUtil.initTableMapperJob(
	       tableName.getName(),
	        scan,
	        Mapper.class,
	        Text.class,
	        DoubleWritable.class,
	        job);
	    // Create output table
	    HBaseAdmin admin = new HBaseAdmin(conf);
	    if (admin.tableExists("A_21805893:Q2")) 
	    {
	    	System.out.println("Table exist");
	    }
	    else 
	    {
	    	TableName tableNameR =  TableName.valueOf("A_21805893:Q2");
	            HTableDescriptor htd = new HTableDescriptor(tableNameR);
	            HColumnDescriptor hcd = new HColumnDescriptor("result");
	            htd.addFamily(hcd);
	            admin.createTable(htd);
	    }
	    // define output table
	    TableMapReduceUtil.initTableReducerJob(
	      "A_21805893:Q2",
	      Reducer.class, 
	      job);
	    
	    boolean b = job.waitForCompletion(true);
	    if (!b) {
	    	throw new IOException("error with job!");
	    }
	  
	   
	  }
}
