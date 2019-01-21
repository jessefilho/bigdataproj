package bdma.bigdata.aiwsbu.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import bdma.bigdata.aiwsbu.Namespace;

import bdma.bigdata.aiwsbu.mapreduce.Question3.Reducer3;
import bdma.bigdata.aiwsbu.mapreduce.Question3.TokenizerMapper3;

public class Question4 {
	private static String tableG = "A_21805893:G";
	private static String tableC = "A_21805893:C";
	
	private static Connection connection = null;

    public static void Setup() {
        try {
            connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        } catch (IOException e) {
            System.err.println("Failed to connect to HBase.");
            System.exit(0);
        }
        
    }

	//MAPPER
	public static class TokenizerMapper4 extends TableMapper<Text, FloatWritable> {
		private final static FloatWritable gradess = new FloatWritable();
		private Text keyy = new Text();

		public void map(ImmutableBytesWritable row,
				Result value,
				Context context) throws InterruptedException, IOException {
			System.out.println("#### MAP ####");
			//System.out.println(value);
			//System.out.println(Bytes.toFloat(value.getValue(Bytes.toBytes("#"),Bytes.toBytes("G"))));

			String key = null;
			System.out.println(Bytes.toString(row.get()));
			String xtr[] = Bytes.toString(row.get()).split("/");
			byte [] value_grades = value.getValue(Bytes.toBytes("#"),Bytes.toBytes("G"));        	    			
			Float grades = Float.valueOf(Bytes.toString(value_grades))/100;			
			System.out.println(grades);
			
			//Grades
			// year/semesterstudent/course
			//2015/072012000123/S07A006

			key = xtr[2]+"/"+xtr[0];
			String ue_course = xtr[2];
			System.out.println(key);

			
			

			Table tableCourse = connection.getTable(TableName.valueOf(tableC));

			// vv PrefixFilterUE
			Filter filter = new PrefixFilter(Bytes.toBytes(xtr[2]));

			Scan scanCourses = new Scan();
			scanCourses.setFilter(filter);
			scanCourses.addColumn(Bytes.toBytes("#"),Bytes.toBytes("N"));
			ResultScanner scannerC = tableCourse.getScanner(scanCourses);
			
			for (Result iC = scannerC.next(); iC != null; iC = scannerC.next()) {//Start FOR iC
				//System.out.println(Bytes.toString(iC.getValue(Bytes.toBytes("#"),Bytes.toBytes("N"))));
				String courseNameFromScan = Bytes.toString(iC.getValue(Bytes.toBytes("#"),Bytes.toBytes("N")));
				key = key+"/"+courseNameFromScan.replace(" ", ";");   			
				
				gradess.set(grades);
				
				try {
					StringTokenizer itr = new StringTokenizer(key);
					while (itr.hasMoreTokens()) {
						keyy.set(itr.nextToken());
						System.out.println(keyy+"   "+gradess);
						context.write(keyy,gradess);		    				    	  
					}


				} catch (InterruptedException e) {
					throw new IOException(e);
				}


			}//End FOR iC
			System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$");
		}// End map
	} // END class Mapper1




	// REDUCER
	//public static class Reducer1 extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable>
	public static class Reducer4 extends TableReducer<Text, FloatWritable, String> {

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context)
						throws IOException, InterruptedException {
			System.out.println("#### REDUCE ####");

			//S04A009/2002/name
			String key_concated = Bytes.toString(key.getBytes());            
			String key_row = key_concated.split("/")[0]+"/"+key_concated.split("/")[1];
			String course_name = key_concated.split("/")[2].replace(";"," ");

			float count = 0;
			//float sum = 0;
			float graded = 0;
			for (FloatWritable val : values) {

				//sum += val.get();
				if(val.get() >= 10) {
					graded++;
				}
				count++;
			}       	

			Float rate = graded/count;        	
			try {
				System.out.println("key:'"+key_row+ "' course name: '"+course_name+"' rate: '"+rate+"'");
				System.out.println("$$$$ PUT $$$$");
				Put put = new Put(key_row.getBytes());
				put.addImmutable(Bytes.toBytes("C"), Bytes.toBytes("NAME"), Bytes.toBytes(course_name));
				put.addImmutable(Bytes.toBytes("#"), Bytes.toBytes("RATE"), Bytes.toBytes(Float.toString(rate)));

				context.write(null,put);
			} catch (Exception e) {
				// TODO: handle exception
			}
			

		}
	}

	//MAIN 
	public static void main(String[] args) throws Exception {
		System.out.println("################# QUESTION 4 - START #################");
		Setup();         

		Configuration conf = connection.getConfiguration();
		Job job = Job.getInstance(conf,"question4_job");		
//		Connection connection = ConnectionFactory.createConnection(conf);			
		//Rules to Students
		
		Table tableCourse = connection.getTable(TableName.valueOf(tableG));
		//Create Table
		TableName tableNameQ4 = TableName.valueOf("A_21805893:Q4");
		Admin hba = connection.getAdmin();
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableNameQ4);
		tableDescriptor.addFamily(new HColumnDescriptor("C"));
		tableDescriptor.addFamily(new HColumnDescriptor("#"));

		if (hba.tableExists(tableNameQ4) == true) {	    	
			hba.disableTable(tableNameQ4);
			System.out.println("Table disable "+ tableNameQ4);
			hba.deleteTable(tableNameQ4);
			System.out.println("Table delete "+ tableNameQ4);		

		}else { 	

			hba.createTable(tableDescriptor);
			System.out.println("Table created "+ tableNameQ4);
		}

		if (hba.tableExists(tableNameQ4) == false) {
			hba.createTable(tableDescriptor);
			System.out.println("Table created "+ tableNameQ4);
		}


		//"S01A001/7984".getBytes(),"S01A005/7982".getBytes()  S01B025/7998 
		System.out.println("############# call Map With limit number row a cause of hardware host limitations ################");
		System.out.println("############# FROM 2017/012001000016/S04A009 TO 2018/012001000016/S04A009 ################");
		//"S01A001/7984".getBytes(),"2002/012001000016/S04A009".getBytes()
		Scan scanGrade = new Scan("S01A001/7984".getBytes(),"S01A005/7982".getBytes());

		job.setJarByClass(Question4.class);
		scanGrade.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scanGrade.setCacheBlocks(false);  // don't set to true for MR jobs

		//        System.out.println("Getting Course "+id+" at Table name " + tableCourse.getName());

		TableMapReduceUtil.initTableMapperJob(
				tableCourse.getName(), // input HBase table name
				scanGrade,// Scan instance to control CF and attribute selection
				TokenizerMapper4.class,// mapper
				Text.class, // mapper output key
				FloatWritable.class, // mapper output value
				job);

		TableMapReduceUtil.initTableReducerJob(
				tableNameQ4.getNameAsString(),      // output table
				Reducer4.class,             // reducer class
				job); 


		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
}
