package bdma.bigdata.aiwsbu.mapreduce;


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

import bdma.bigdata.aiwsbu.mapreduce.Question3.Mapper3;
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

	//MAPPER
	static class Mapper1 extends TableMapper<Text, Text> {
		private Text keyy = new Text();
	    private Text valuee = new Text();
        public void map(ImmutableBytesWritable row,
        		Result values,
        		Context context) throws IOException, InterruptedException {
        	System.out.println("#### MAP ####");
        	
        	
        	Configuration conf = HBaseConfiguration.create();
        	Connection connection = ConnectionFactory.createConnection(conf);
        	//Get tables
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

    		
    		Scan scanStudents = new Scan(values.getRow());
        	ResultScanner scannerS = tableStudent.getScanner(scanStudents);
        	
//        	for (Result iStudent = scannerS.next(); iStudent != null; iStudent = scannerS.next()) { // Start For iStudent
//        		System.out.println(Bytes.toString(iStudent.getRow()));
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
        			    	value = value+Bytes.toString(nameCourse_byte)+"/";
        			    }
    			    	//semesterFirst.add(2,String.valueOf(grades));//Grades values
    			    	value = value+String.valueOf(grades);
    			    	
    			    	System.out.println(key);
    			    	System.out.println(value);	
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
//        	}// End For iStudent
        	
    		}// End map

        
     } // END class Mapper1
    

	
	
	// REDUCER
	//public static class Reducer1 extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable>
	public static class Reducer1 extends TableReducer<Text,Text,String> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        	try {
				
			} catch (Exception e) {
				System.out.println(e);
			}
        	System.out.println("#### REDUCE ####");
        	System.out.println(key +" "+values);
        	//context.write();
        	
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//            
//            Put put = new Put(key.toString().getBytes());
//            put.addImmutable(Bytes.toBytes("details"), Bytes.toBytes("total"), Bytes.toBytes(sum));
//            System.out.println(String.format("stats :   key : %d,  count : %d", Bytes.toInt(key.toString().getBytes()), sum));
//            //context.write(key, put);
//            context.write(key, put);
        }
    }
	
	//MAIN 
	public static void main(String[] args) throws Exception {
		System.out.println("################# QUESTION 1 - START #################");
		Configuration conf = HBaseConfiguration.create();		
		Job job = Job.getInstance(conf,"question1_job");
		
	    Connection connection = ConnectionFactory.createConnection(conf);		
		Table tableStudent = connection.getTable(TableName.valueOf(tableS));

        Scan scanStudent = new Scan("2001000002".getBytes(),"2001000003".getBytes());
        scanStudent.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scanStudent.setCacheBlocks(false);  // don't set to true for MR jobs
        
        TableMapReduceUtil.initTableMapperJob(
        		tableStudent.getName(), // input HBase table name
        		scanStudent, // Scan instance to control CF and attribute selection
        		Mapper1.class, // mapper
        		Text.class,// mapper output key
        		Text.class,
        		job);// mapper output value
        
        job.setJarByClass(Question1.class);
        job.setCombinerClass(Reducer1.class);
        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        //job.setOutputFormatClass(NullOutputFormat.class);
//        TableMapReduceUtil.initTableReducerJob(
//        		targetTable,      // output table
//        		null,             // reducer class
//        		job);
//        TableMapReduceUtil.initTableReducerJob(
//        		id,
//        		Reducer1.class,        		
//        		job);
        //TableMapReduceUtil.initTableReducerJob(tableStudent, Reducer1.class, job);
	    // Delete output if exists
	    FileSystem hdfs = FileSystem.get(conf);
	    if (hdfs.exists(new Path("file:///localhost:9000/home/hadoop/out")))
	      hdfs.delete(new Path("file:///localhost:9000/home/hadoop/out"), true);
		FileOutputFormat.setOutputPath(job, new Path("file:///localhost:9000/home/hadoop/out"));
        
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean b = job.waitForCompletion(true);
		if (!b) {
		  throw new IOException("error with job!");
		}
    }
}