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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.omg.CORBA.portable.ValueOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;




public class Question1 {
	
//	private static Connection connection = null;
	private static String tableS = "A_21805893:S";
	private static String tableG = "A_21805893:G";
	private static String tableC = "A_21805893:C";
//    public void Question1() {
//        try {
//            connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
//        } catch (IOException e) {
//            System.err.println("Failed to connect to HBase.");
//            System.exit(0);
//        }catch ( NullPointerException e) {
//            System.err.println("NullPointerException Caught - Failed to connect to HBase.");
//            System.exit(0);
//        }
//        
//    }

	//MAPPER
	static class Mapper1 extends TableMapper<ImmutableBytesWritable, List<String>> {
        public void map(ImmutableBytesWritable row,
        		Result values,
        		Context context) throws IOException {
        	System.out.println("#### MAP ####");
        	System.out.println("Processing ...");
        	//{"Name":"Jean DUPOND",
        	// "Email":"jean.dupond@univ-blois.fr",
        	// "Program":"M1",
        	
        	//	"First":[{"Code":"S07A001","Name":"Big Data","Grade":"17.5"},{...},...],
        	//	"Second":[{"Code":"S08A001","Name":"Database","Grade":"6.25"},{...},...]}
        	
        	Configuration conf = HBaseConfiguration.create();
        	Connection connection = ConnectionFactory.createConnection(conf);
        	//Get tables
    		Table tableGrade = connection.getTable(TableName.valueOf(tableG));
    		Table tableCourse = connection.getTable(TableName.valueOf(tableC));
    		String id = Bytes.toString(values.getRow()) ;
        	
        	//System.out.println(row);
        	//System.out.println(values);
        	
        	//System.out.println(values.getValue(Bytes.toBytes("C"), Bytes.toBytes("B")));
        	//System.out.println(values.getColumnCells(Bytes.toBytes("#"),Bytes.toBytes("F")));
        	
        	
        	
        	
        	//Grades
        	// year/semesterstudent/course
        	//2015/072012000123/S07A006
    		System.out.print(" ...");
    		Scan scanGrades = new Scan();    	
    		scanGrades.addColumn(Bytes.toBytes("#"),Bytes.toBytes("G"));
    		ResultScanner scannerG = tableGrade.getScanner(scanGrades);
    		
    		// Course
    		
    		
    		
//    		Scan scanCourses = new Scan();
//    		scanCourses.addColumn(Bytes.toBytes("#"),Bytes.toBytes("N")); // Course name
//    		ResultScanner scannerC = tableCourse.getScanner(scanCourses);
    		
    		 
    		List<String> semesterFirst = new ArrayList<>();
    		List<String> semesterSecond = new ArrayList<>();
    		List<String> first = new ArrayList<>();
    		List<String> second = new ArrayList<>();
    		List<String> root = new ArrayList<>();
    		
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
    		root.add(0, name);
    		
    		if(email == "") { // if email is empty
    			root.add(1, "none");
    		}else {
    			root.add(1, email);
    		}
    		
    		switch(Integer.valueOf(Program)) {
    		  case 1: // L1
    		    // code block
    			  root.add(2, "L"+Program);
    		    break;
    		  case 2: // L2
    		    // code block
    			  root.add(2, "L"+Program);
    		    break; // L3
    		  case 3:
    			  root.add(2, "L"+Program);
      		    // code block
      		    break; // M1
    		  case 4:
      		    // code block
    			  root.add(2, "M1");
      		    break;
    		  case 5: // M2
      		    // code block
    			  root.add(2, "M2");
      		    break;
    		  default:
    		    // code block
    			  root.add(2, "none");
    		}
    		
    		
    		System.out.print(" ...");
    		
    		for (Result iG = scannerG.next(); iG != null; iG = scannerG.next()) {
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
    				
    				// it not contents every year -- ERRO OF LOGIC on Load scripts
    				String year = pointer.get(0).toString().split("/")[0];
    				String yCode = String.valueOf(9999 - Integer.valueOf(year));    				
    				String courseCode = pointer.get(0).toString().split("/")[2]; //course id
    				String courseID =  courseCode+"/"+ yCode;
    				
    				Get getC =  new Get(Bytes.toBytes(courseID));
    				getC.addColumn(Bytes.toBytes("#"),Bytes.toBytes("N")); // Course Name
    				Result resultCourses = tableCourse.get(getC);
    				System.out.println(resultCourses);
    			    byte [] nameCourse_byte = resultCourses.getValue(Bytes.toBytes("#"),Bytes.toBytes("N"));
    			    
    			    //System.out.println("course "+courseID);
    			    //System.out.println(Bytes.toString(nameCourse_byte));
    			    
    			    if ((Integer.valueOf(sem_stud_id.substring(0,2)) % 2) != 0) { // First Semester
    			    	semesterFirst.add(0,courseCode);// Course code
    			    	
    			    	if(Bytes.toString(nameCourse_byte) == null) { // if it has not a course name
    			    		semesterFirst.add(1, "none");
        			    }else {
        			    	semesterFirst.add(1, Bytes.toString(nameCourse_byte));
        			    }
    			    	semesterFirst.add(2,String.valueOf(grades));//Grades values
    				} else {// Second Semester
    					semesterSecond.add(0,courseCode);// Course code
    					
    					if(Bytes.toString(nameCourse_byte) == null) { // if it has not a course name
    						semesterSecond.add(1, "none");
        			    }else {
        			    	semesterSecond.add(1, Bytes.toString(nameCourse_byte));
        			    }
    					semesterSecond.add(2,String.valueOf(grades)); // Grades values
    				}
    			    if (!semesterFirst.isEmpty()) {
    			    	first.add(semesterFirst.stream()
    			    			.collect(Collectors.joining(",", "{", "}")));
    			    	
					}
    			    if (!semesterSecond.isEmpty()) {
    			    	second.add(semesterSecond.stream()
    			    			.collect(Collectors.joining(",", "{", "}")));
					}
    			    semesterFirst = new ArrayList<>();
    			    semesterSecond = new ArrayList<>();
//    				String a = String.join(",", semesterFirst);
//    				root.add(4,a);
    				//break;
    			    } // End If ids
    				
    				
    			}// End For iG
    		//{"Name":"Jean DUPOND",
        	// "Email":"jean.dupond@univ-blois.fr",
        	// "Program":"M1",
        	
        	//	"First":[{"Code":"S07A001","Name":"Big Data","Grade":"17.5"},{...},...],
        	//	"Second":[{"Code":"S08A001","Name":"Database","Grade":"6.25"},{...},...]}
    		
    		//System.out.println(semesterFirst);
			//System.out.println(semesterSecond);    
//    		System.out.println(first);
//    		System.out.println(second);
//    		System.out.println(first.stream()
//	    			.collect(Collectors.joining(",", "[", "]")));
    		System.out.print(" ...");
    		root.add(3,first.stream()
	    			.collect(Collectors.joining(",", "[", "]")));
    		System.out.print(" ...");
    		root.add(4,second.stream()
	    			.collect(Collectors.joining(",", "[", "]")));
    		System.out.println(" ... Done!");
			System.out.println(root);
    		ImmutableBytesWritable userKey = new ImmutableBytesWritable(values.getRow());
    		try {
    			  context.write(userKey,root);
	          } catch (InterruptedException e) {
	              throw new IOException(e);
	          }
			
    		}// End map
        	
        	
        	// extract userKey from the compositeKey (userId + counter)
        	
    	    
//            ImmutableBytesWritable userKey = new ImmutableBytesWritable(row.get(), 0, Bytes.SIZEOF_INT);
//            try {
//                context.write(userKey, one);
//            } catch (InterruptedException e) {
//                throw new IOException(e);
//            }
//            numRecords++;
//            if ((numRecords % 10000) == 0) {
//                context.setStatus("mapper processed " + numRecords + " records so far");
//            }
        
        
     } // END class Mapper1
    

	
	
	// REDUCER
	//public static class Reducer1 extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable>
	public static class Reducer1 extends TableReducer<String,List<String>,Text> {

        public void reduce(String key, Iterable<List<String>> values, Context context)
                throws IOException, InterruptedException {
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
		
	    // Delete output if exists
//	    FileSystem hdfs = FileSystem.get(conf);
//	    if (hdfs.exists(new Path("/home/hadoop/out")))
//	      hdfs.delete(new Path("/home/hadoop/out"), true);
		FileOutputFormat.setOutputPath(job, new Path("/home/hadoop/out"));
	    Connection connection = ConnectionFactory.createConnection(conf);
		//input
		String id = "2015001000";// student id
		String program = "P";
		//Rules to Students
		Table tableStudent = connection.getTable(TableName.valueOf(tableS));
		
	    Get get =  new Get(Bytes.toBytes(id)); // Get row
	    get.addColumn(Bytes.toBytes("#"),Bytes.toBytes(program));
	    get.addColumn(Bytes.toBytes("#"),Bytes.toBytes("F")); // First name
	    get.addColumn(Bytes.toBytes("#"),Bytes.toBytes("L")); // First name
	    get.addColumn(Bytes.toBytes("C"),Bytes.toBytes("E")); // First name
//	    Result result = table.get(get);
//	    byte [] value = result.getValue(Bytes.toBytes("#"),Bytes.toBytes("F"));
//	    byte [] value1 = );
//	    String name = Bytes.toString(value);
//	    String last = Bytes.toString(value1);
	    
        Scan scanStudent = new Scan(get);
        scanStudent.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scanStudent.setCacheBlocks(false);  // don't set to true for MR jobs
        System.out.println("############# call Map With ################");
        System.out.println("Getting Student "+id+" at Table name " + tableStudent.getName());
        
        TableMapReduceUtil.initTableMapperJob(
        		tableStudent.getName(),
        		scanStudent,
        		Mapper1.class,
        		ImmutableBytesWritable.class,
        		IntWritable.class, job);
        
        job.setJarByClass(Question1.class);        
		
		
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(List.class);
	    
	    //job.setMapOutputKeyClass(TextOutputFormat.class);
	    //job.setMapOutputValueClass(TextOutputFormat.class);
	    
	    
	    //job.setOutputFormatClass();
	    
//        TableMapReduceUtil.initTableReducerJob(
//        		id,
//        		Reducer1.class,        		
//        		job);
        //TableMapReduceUtil.initTableReducerJob(tableStudent, Reducer1.class, job);

        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}