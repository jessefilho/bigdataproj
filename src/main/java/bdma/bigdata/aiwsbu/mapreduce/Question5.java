package bdma.bigdata.aiwsbu.mapreduce;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;


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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;




public class Question5 {
	
	private static String tableG = "A_21805893:G";
	private static String tableC = "A_21805893:C";
	public static  Map<String, String> CreateHashmapFromTableCourse (Connection connection) throws IOException
	{
     Map<String, String> map = new HashMap<>();
	 ResultScanner resultScanner = null;
     Table tableInter = connection.getTable(TableName.valueOf(tableC));
     Scan scan1 = new Scan();
     scan1.addColumn(Bytes.toBytes("#"), Bytes.toBytes("N"));
     resultScanner = tableInter.getScanner(scan1);
     for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
     	byte[] RowKey=result.getRow();
     	String key = Bytes.toString(RowKey).split("/")[0];
        byte[] Value = result.getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));
        String value = Bytes.toString(Value);
        map.put(key, value);
	}
     return map;
	}
	public static Map<String, Double> CreateHashmapFromIntermResult (Connection connection) throws IOException
	{
     Map<String, Double> map = new HashMap<>();
	 ResultScanner resultScanner = null;
     Table tableInter = connection.getTable(TableName.valueOf("InterResultTableQ5"));
     Scan scan1 = new Scan();
     scan1.addColumn(Bytes.toBytes("result"), Bytes.toBytes("average"));
     resultScanner = tableInter.getScanner(scan1);
     for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
     	byte[] RowKey=result.getRow();
     	String key = Bytes.toString(RowKey);
        byte[] Value = result.getValue(Bytes.toBytes("result"), Bytes.toBytes("average"));
        double Avgrades = ByteBuffer.wrap(Value).getDouble();
        map.put(key, Avgrades);
	}
     return map;
	}
	public static void CreateHbaseTable (Configuration conf,String tablename, String ColumnFamily  ) throws IOException 
	{
		 
		  HBaseAdmin admin = new HBaseAdmin(conf);
  	    if (admin.tableExists(tablename)) 
  	    {
  	    	System.out.println("Table exist");
  	    }
  	    else 
  	    {
  	    	TableName tableNameR =  TableName.valueOf(tablename);
	            HTableDescriptor htd = new HTableDescriptor(tableNameR);
	            HColumnDescriptor hcd = new HColumnDescriptor(ColumnFamily);
	            htd.addFamily(hcd);
	            admin.createTable(htd);
  	    }
	}
	public static String CheckParameters1(Connection connection ,String param1) throws IOException 
	{
		 Set<String> hash_Set = new HashSet<String>(); 
		String sem1,sem2;
		String exist =null;
		ResultScanner resultScanner = null;
  	    Table tableInter = connection.getTable(TableName.valueOf(tableG));
  	    Scan scan1 = new Scan();
        scan1.addColumn(Bytes.toBytes("#"), Bytes.toBytes("G"));
        resultScanner = tableInter.getScanner(scan1);
          for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
          	byte[] StudentRowKey=result.getRow();
          	String key = Bytes.toString(StudentRowKey);
            // get year from rowKey and convert it to string
 		    String year =key.split("/")[0];
 		   hash_Set.add(year);
          }
          for (String year : hash_Set) {
        	  if (year.equals(param1))
        		  exist="exist";
        	}
           
          return exist;
          }
	public static String CheckParameters2(String param2) throws IOException 
	{
		 String exist=null;
		 if (param2.equals("L1") || param2.equals("L2") || param2.equals("L3") ||param2.equals("M1") ||param2.equals("M2"))
				 exist="exist";
           
          return exist;
    }
	public static class Mapper extends TableMapper <Text, IntWritable>{
		 public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
		   throws IOException, InterruptedException {

		  try {
		    String sem1,sem2;
		   //passing parameters to map
		   Configuration conf = context.getConfiguration();
		   String param1 = conf.get("Year");
		   String param2 = conf.get("Promotion");
	
		   // get rowKey and convert it to string
		   String inKey = new String(rowKey.get());
	       // get year from rowKey and convert it to string
		   String year =inKey.split("/")[0];
		   // get semester from rowKey and convert it to string
		   String semester =inKey.split("/")[2].substring(0,3);
		   switch(param2) {
	         case "L1" :
	        	 sem1="S01";
	        	 sem2="S02";
	        	 
	        	 if (!param1.equals(year) && !sem1.equals(semester) && !sem2.equals(semester))
	  		   {
	        		// System.out.println("Invalid Year or Promotion"); 
	  			   return;
	  		   } 
	        	 else if (param1.equals(year) && (sem1.equals(semester) || sem2.equals(semester)))
	           {
	        	   // set new output key having  UE ID
	      		   String oKey = inKey.split("/")[2];
	      		   // get grades as output value 
	      		   byte[] bGrades = columns.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));
	      		   String sGrades = new String(bGrades);
	      		   Integer grades = new Integer(sGrades);
	      		   context.write(new Text(oKey),new IntWritable(grades));
	           }
	            break;
	         case "L2" :
	        	 sem1="S03";
	        	 sem2="S04";
	        	 if (!param1.equals(year) && !sem1.equals(semester) && !sem2.equals(semester))
		  		   {
	        		  
		  			   return;
		  		   } 
		        	 else if (param1.equals(year) && (sem1.equals(semester) || sem2.equals(semester)))
		           {
	        		 // set new output key having  UE ID
		      		   String oKey = inKey.split("/")[2];
		      		   // get grades as output value 
		      		   byte[] bGrades = columns.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));
		      		   String sGrades = new String(bGrades);
		      		   Integer grades = new Integer(sGrades);
		      		   context.write(new Text(oKey),new IntWritable(grades));
		           }
	        	 
	         case "L3" :
	        	 sem1="S05";
	        	 sem2="S06";
	        	 if (!param1.equals(year) && !sem1.equals(semester) && !sem2.equals(semester))
		  		   {
	        		   
		  			   return;
		  		   } 
		        	 else if (param1.equals(year) && (sem1.equals(semester) || sem2.equals(semester)))
		           {
		        	  // set new output key having  UE ID
			      	  String oKey = inKey.split("/")[2];
		      		   // get grades as output value 
		      		   byte[] bGrades = columns.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));
		      		   String sGrades = new String(bGrades);
		      		   Integer grades = new Integer(sGrades);
		      		   context.write(new Text(oKey),new IntWritable(grades));
		           }
	            break;
	         case "M1" :
	        	 sem1="S07";
	        	 sem2="S08";
	        	 if (!param1.equals(year) && !sem1.equals(semester) && !sem2.equals(semester))
		  		   {
	        		   
		  			   return;
		  		   } 
		        	 else if (param1.equals(year) && (sem1.equals(semester) || sem2.equals(semester)))
		           {
		        	  // set new output key having  UE ID
			      	  String oKey = inKey.split("/")[2];
		      		   // get grades as output value 
		      		   byte[] bGrades = columns.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));
		      		   String sGrades = new String(bGrades);
		      		   Integer grades = new Integer(sGrades);
		      		   context.write(new Text(oKey),new IntWritable(grades));
		           }
	         case "M2" :
	        	 sem1="S09";
	        	 sem2="S10";
	        	 if (!param1.equals(year) && !sem1.equals(semester) && !sem2.equals(semester))
		  		   {   
	        		   
		  			   return;
		  		   } 
		        	 else if (param1.equals(year) && (sem1.equals(semester) || sem2.equals(semester)))
		           {
		        	  // set new output key having  UE ID
			      	   String oKey = inKey.split("/")[2];
		      		   // get grades as output value 
		      		   byte[] bGrades = columns.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));
		      		   String sGrades = new String(bGrades);
		      		   Integer grades = new Integer(sGrades);
		      		   context.write(new Text(oKey),new IntWritable(grades));
		           }
	            break;
	         default :
	            System.out.println("Invalid Promotion");
	      }
		   
		 
		  } catch (RuntimeException e){
		   e.printStackTrace();
		  }
		 }
		}

	public  static class Reducer  extends TableReducer <Text, IntWritable, ImmutableBytesWritable> {
		 
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		  try {
		   DecimalFormat df2 = new DecimalFormat(".##");
		   double average;
		   int sum = 0;
		   int total = 0;
		   for (IntWritable grade: values) {
		    Integer intGrades = new Integer(grade.toString());
		    total += 1;
		    sum += intGrades;
		   } 
		   // Calculate average of grades of UE for a given year
		   average=sum/total;
		   // create hbase put with rowkey as year
		   Put insHBase = new Put(key.getBytes());
		   // insert average value to hbase 
		   insHBase.add(Bytes.toBytes("result"), Bytes.toBytes("average"), Bytes.toBytes(average));
		   // write data to Hbase table
		   context.write(null, insHBase);
		    System.out.println("UE Id"+""+key +""+"average"+""+average);

		  } catch (Exception e) {
		   e.printStackTrace();
		  }
		 }
	}
	public static  void main(String[] args) throws Exception {
		 Map<String, String> CourseMap = new HashMap<>();
		 Map<String, Double> InterResultMap = new HashMap<>();
		 Map<String,Double> ResultMap = new HashMap<>();
   	 Configuration conf = HBaseConfiguration.create();
   	 Connection connection = ConnectionFactory.createConnection(conf);
   	 System.out.println("Enter a year");
   	 Scanner scanner = new Scanner(System. in); 
   	 String input = scanner. nextLine();
   	 System.out.println("Enter a promotion L1/L2/L3/M1/M2");
   	 String input2 = scanner. nextLine();
   	 //passing parameters
   	 conf.set("Year", input);
   	 conf.set("Promotion", input2);
   	 //Check rows with these  parameters exists or not 
   	 String checkParam1 =CheckParameters1(connection ,input);
   	 String checkParam2 =CheckParameters2(input2);
   	 if (checkParam1!=null && checkParam2!=null)
   	 {
   	 Scan scan = new Scan();
   	 Job job = Job.getInstance(conf,"JobQ5"); 
   	 job.setJarByClass(Question5.class);
   	 //define input hbase table
   	    TableMapReduceUtil.initTableMapperJob(
   	       tableG,
   	        scan,
   	        Mapper.class,
   	        Text.class,
   	        IntWritable.class,
   	        job);
   	   
   	    // Create intermediate output table
   	    CreateHbaseTable (conf,"A_21805893:Q5","result");
   	  
   	    // Define output table
   	    TableMapReduceUtil.initTableReducerJob(
   	      "A_21805893:Q5",
   	      Reducer.class, 
   	      job);
   	    boolean b = job.waitForCompletion(true);
   	    if (!b) {
   	    	throw new IOException("error with job!");
   	    }
   	      
   	 }
   	 else
   	 System.out.println("NOT FOUND");
   	  
 
 
 }
}

