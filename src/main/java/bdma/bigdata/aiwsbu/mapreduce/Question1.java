package bdma.bigdata.aiwsbu.mapreduce;


import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.omg.CORBA.portable.ValueOutputStream;

import com.sun.org.apache.commons.logging.LogFactory;


import bdma.bigdata.aiwsbu.mapreduce.Question3.Reducer3;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collector;
import java.util.stream.Collectors;




public class Question1 {


	private static String tableS = "A_21805893:S";
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
	static class Mapper1 extends TableMapper<Text, Text> {
		private Text keyy = new Text();
		private Text valuee = new Text();    

		public void map(ImmutableBytesWritable row,
				Result values,
				Context context) throws IOException, InterruptedException {
			System.out.println("#### MAP ####");   	

			//Connect to tables
			Table tableGrade = connection.getTable(TableName.valueOf(tableG));
			Table tableCourse = connection.getTable(TableName.valueOf(tableC));
			Table tableStudent = connection.getTable(TableName.valueOf(tableS));

			System.out.println(Bytes.toString(values.getRow()));
			//Grades
			// year/semesterstudent/course
			//2015/072012000123/S07A006

			Scan scanGrades = new Scan();    	
			scanGrades.addColumn(Bytes.toBytes("#"),Bytes.toBytes("G"));
			ResultScanner scannerG = tableGrade.getScanner(scanGrades);

			String key = null;
			String id = Bytes.toString(values.getRow()); //Bytes.toString(iStudent.getRow());

			key = id+"/";

			//semesterFirst have:
			// From Course a -> "Code":"S07A001"
			// From Course a -> "Name":"Database"
			// From Grade a -> "Grade":"17.5" 

			String firstName = Bytes.toString(values.getValue(Bytes.toBytes("#"), Bytes.toBytes("F")));
			String lastName = Bytes.toString(values.getValue(Bytes.toBytes("#"), Bytes.toBytes("L")));
			String name =  firstName +" "+ lastName;
			String email = Bytes.toString(values.getValue(Bytes.toBytes("C"), Bytes.toBytes("E")));
			String Program = Bytes.toString(values.getValue(Bytes.toBytes("#"), Bytes.toBytes("P")));;


			System.out.print(" ...");

			key = key + name.replace(" ",";")+"/";
			if(email == "") { // if email is empty

				key = key+"none"+"/";
			}else {

				key = key+email+"/";
			}
			//	    		System.out.println("########");
			//	    		System.out.println(key);
			//	    		System.out.println(Integer.valueOf(Program));
			switch(Integer.valueOf(Program)) {
			case 1: // L1	    		    
				key = key+"L"+Program;
				break;
			case 2: // L2	    		    
				key = key+"L"+Program;
				break;
			case 3: // L3	    			 
				key = key+"L"+Program;

				break; 
			case 4:// M1

				key = key+"M1";
				break;
			case 5: // M2

				key = key+"M2";
				break;
			default:	    		   
				key = key+"none";
			}
			//	    		System.out.println(key);
			//	    		System.out.println(Integer.valueOf(Program));

			for (Result iG = scannerG.next(); iG != null; iG = scannerG.next()) {
				String value = null;
				List<Cell> pointer = iG.getColumnCells(Bytes.toBytes("#"), Bytes.toBytes("G"));
				byte [] value_grades = iG.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));    			
				Float grades = Float.valueOf(Bytes.toString(value_grades))/100;

				String sem_stud_id = pointer.get(0).toString().split("/")[1];    			
				String stud_id = sem_stud_id.substring(2,12);
				//semesterFirst have:
				// From Course a -> "Code":"S07A001"
				// From Course a -> "Name":"Database"
				// From Grade a -> "Grade":"17.5" 

				//Built array Semesters
				if (id.equals(stud_id)) {    
					System.out.println(id+" $$$$ MATCH $$$$ " +stud_id);

					// it not contents every year -- ERRO OF LOGIC on Load scripts
					String year = pointer.get(0).toString().split("/")[0];
					String yCode = String.valueOf(9999 - Integer.valueOf(year));    				
					String courseCode = pointer.get(0).toString().split("/")[2]; //course id
					String courseID =  courseCode+"/"+ yCode;

					Get getC =  new Get(Bytes.toBytes(courseID));
					getC.addColumn(Bytes.toBytes("#"),Bytes.toBytes("N")); // Course Name
					Result resultCourses = tableCourse.get(getC);

					// key : id student/name/email/M1
					// values : code/name_course/grade

					byte [] nameCourse_byte = resultCourses.getValue(Bytes.toBytes("#"),Bytes.toBytes("N"));

					//System.out.println("course "+courseID);
					//System.out.println(Bytes.toString(nameCourse_byte));


					value = courseCode+"/";
					if(Bytes.toString(nameCourse_byte) == null) { // if it has not a course name
						//semesterFirst.add(1, "none");
						value = value+"none"+"/";
					}else {
						//semesterFirst.add(1, Bytes.toString(nameCourse_byte));
						value = value+Bytes.toString(nameCourse_byte).replace(" ",";")+"/";
					}
					//semesterFirst.add(2,String.valueOf(grades));//Grades values
					value = value+String.valueOf(grades);
					//    			    	System.out.println("$$$$$$$$$$$$$$");
					//    			    	System.out.println(key);
					//    			    	System.out.println(value);
					try {
						StringTokenizer itr = new StringTokenizer(key.replace(" ", ";"));
						while (itr.hasMoreTokens()) {
							keyy.set(itr.nextToken());		    				    	  
							valuee.set(value); 
							context.write(keyy,valuee);		    				    	  
						}
					} catch (InterruptedException e) {
						throw new IOException(e);
					} 			


				} // End If ids

			}// End For iG


		}// End map


	} // END class Mapper1




	// REDUCER
	//public static class Reducer1 extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable>
	public static class Reducer1 extends TableReducer<Text,Text,ImmutableBytesWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			System.out.println("#### REDUCE ####");
			//System.out.println(key +" "+values);
			String key_concated = Bytes.toString(key.getBytes());
			System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
			System.out.println(key_concated.split("/")[0]);
			System.out.println(key_concated.split("/")[2]);
			System.out.println(key_concated.split("/")[3].substring(0,2));

			Put put = new Put(key_concated.split("/")[0].getBytes());
			put.addImmutable(Bytes.toBytes("S"), Bytes.toBytes("NAME"), Bytes.toBytes(key_concated.split("/")[1].replace(";", " ")));
			put.addImmutable(Bytes.toBytes("S"), Bytes.toBytes("EMAIL"), Bytes.toBytes(key_concated.split("/")[2]));
			put.addImmutable(Bytes.toBytes("S"), Bytes.toBytes("PROGRAM"), Bytes.toBytes(key_concated.split("/")[3].substring(0,2)));
			for ( Text val : values) {
				//System.out.println(key);
				String text_concated = Bytes.toString(val.getBytes());
				String c_semester = text_concated.split("/")[0];
				String c_name = text_concated.split("/")[1].replace(";"," ");
				String s_grade = text_concated.split("/")[2].substring(0,4);
				put.addImmutable(Bytes.toBytes("C"), Bytes.toBytes("CODE"), Bytes.toBytes(c_semester));
				put.addImmutable(Bytes.toBytes("C"), Bytes.toBytes("NAME"), Bytes.toBytes(c_name));
				put.addImmutable(Bytes.toBytes("C"), Bytes.toBytes("GRADE"), Bytes.toBytes(s_grade));
			}

			context.write(null, put);

		}
	}

	//MAIN 
	public static void main(String[] args) throws Exception {
		System.out.println("################# QUESTION 1 - START #################");
		Setup(); 

		Configuration conf = connection.getConfiguration();		
		Job job = Job.getInstance(conf,"question1_job");

		Connection connection = ConnectionFactory.createConnection(conf);		
		Table tableStudent = connection.getTable(TableName.valueOf(tableS));

		//Create Table
		TableName tableNameQ1 = TableName.valueOf("A_21805893:Q1");
		Admin hba = connection.getAdmin();
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableNameQ1);
		tableDescriptor.addFamily(new HColumnDescriptor("S"));
		tableDescriptor.addFamily(new HColumnDescriptor("C"));

		if (hba.tableExists(tableNameQ1) == true) {	    	
			hba.disableTable(tableNameQ1);
			System.out.println("Table disable "+ tableNameQ1);
			hba.deleteTable(tableNameQ1);
			System.out.println("Table delete "+ tableNameQ1);		

		}else { 	

			hba.createTable(tableDescriptor);
			System.out.println("Table created "+ tableNameQ1);
		}

		if (hba.tableExists(tableNameQ1) == false) {
			hba.createTable(tableDescriptor);
			System.out.println("Table created "+ tableNameQ1);
		}
		System.out.println("############# call Map With limit number row a cause of hardware host limitations ################");
		System.out.println("############# FROM 2017000291 TO 2017000315 ################");
		//"2017000291".getBytes(),"2017000315".getBytes()
		Scan scanStudent = new Scan("2017000291".getBytes(),"2017000308".getBytes());
		job.setJarByClass(Question1.class);
		scanStudent.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scanStudent.setCacheBlocks(false);  // don't set to true for MR jobs

		job.setJarByClass(Question1.class);

		TableMapReduceUtil.initTableMapperJob(
				tableStudent.getName(), // input HBase table name
				scanStudent, // Scan instance to control CF and attribute selection
				Mapper1.class, // mapper
				Text.class,// mapper output key
				Text.class,
				job);// mapper output value

		TableMapReduceUtil.initTableReducerJob(
				"A_21805893:Q1",      // output table
				Reducer1.class,             // reducer class
				job);

		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
}