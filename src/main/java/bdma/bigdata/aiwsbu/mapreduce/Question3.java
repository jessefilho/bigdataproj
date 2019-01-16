package bdma.bigdata.aiwsbu.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.codec.CellCodecWithTags;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.omg.CORBA.portable.ValueOutputStream;

import bdma.bigdata.aiwsbu.mapreduce.Question1.Reducer1;
import bdma.bigdata.aiwsbu.mapreduce.WordCount.IntSumReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collector;
import java.util.stream.Collectors;




public class Question3 {

	
	private static String tableG = "A_21805893:G";
	private static String tableC = "A_21805893:C";
	private static String tableS = "A_21805893:S";

	//MAPPER
	public static class TokenizerMapper3 extends TableMapper<Text, FloatWritable> {
		private final static FloatWritable gradess = new FloatWritable();
	    private Text keyy = new Text();
	    
        public void map(ImmutableBytesWritable row,
        		Result value,
        		Context context) throws InterruptedException, IOException {
        	System.out.println("#### MAP ####");     	
        	System.out.println(Bytes.toString(value.getRow()));
        	String key = null;

        	//Grades
        	// year/semesterstudent/course
        	//2015/072012000123/S07A006
    		
    		Configuration conf = HBaseConfiguration.create();
        	Connection connection = ConnectionFactory.createConnection(conf);
    		Table tableGrade = connection.getTable(TableName.valueOf(tableG));
    		Scan scanGrades = new Scan();    	
    		scanGrades.addColumn(Bytes.toBytes("#"),Bytes.toBytes("G"));
    		ResultScanner scannerG = tableGrade.getScanner(scanGrades);
    		
    		
    		
    		Table tableCourse = connection.getTable(TableName.valueOf(tableC));
    		Scan scanCourses = new Scan();    	
    		scanCourses.addColumn(Bytes.toBytes("#"),Bytes.toBytes("N"));
    		ResultScanner scannerC = tableCourse.getScanner(scanCourses);
    		
    		int count = 0;
        	for (Cell cell : value.rawCells()){ // start for cell to get each set from scan
        		System.out.println(count++);
        		String str = new String(CellUtil.cloneRow(cell).clone());        		
        		String [] ue_course = str.split("/");// Split Row        		
        		//System.out.println(ue_course[0]);// Get code UE
        		
        		String courseName = Bytes.toString(CellUtil.cloneValue(cell));
        		
        		
    		for (Result iC = scannerC.next(); iC != null; iC = scannerC.next()) {//Start FOR iC
    			
    			if (ue_course[0].equals(Bytes.toString(iC.getRow()).split("/")[0])) {
    				String courseNameFromScan = Bytes.toString(iC.getValue(Bytes.toBytes("#"),Bytes.toBytes("N")));
    				//key = courseNameFromScan;//ue_course[0]+"/"+courseNameFromScan;    			
    				key = ue_course[0]+"/"+courseNameFromScan;
		    		for (Result iG = scannerG.next(); iG != null; iG = scannerG.next()) {//Start FOR iG
		    			
		    			String ueFromGrades = Bytes.toString(iG.getRow()).split("/")[2];
		    			byte [] value_grades = iG.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));    			
		    			Float grades = Float.valueOf(Bytes.toString(value_grades))/100;
		    			
		    			if (ue_course[0].equals(ueFromGrades)) {
		    				//gradesList.add(grades);
		    				//System.out.println(str +" "+key+" "+grades);
		    				gradess.set(grades);
		    				
		    				try {
		    					StringTokenizer itr = new StringTokenizer(key.replace(" ", ";"));
		    				      while (itr.hasMoreTokens()) {
		    				    	  keyy.set(itr.nextToken());
		    				    	  
		    				    	  context.write(keyy,gradess);		    				    	  
		    				      }
				                
				                
				            } catch (InterruptedException e) {
				                throw new IOException(e);
				            }
						}
		    			
		    				
		    		}//End FOR iG
    			} // end for cell 
//		    		System.out.println(key);
//		    		System.out.println(gradesList);
//		    		try {
//		                context.write(key, gradesList);
//		            } catch (InterruptedException e) {
//		                throw new IOException(e);
//		            }
//		    		gradesList = new ArrayList<>();
    			}// if ue from course
    			
    		}//End FOR iC    		
    		
    		}// End map
        	
        	
        
        
     } // END class Mapper1
    

	
	
	// REDUCER
	//public static class Reducer1 extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable>
	public static class Reducer3 extends TableReducer<Text, FloatWritable, String> {

        public void reduce(Text key, Iterable<FloatWritable> values,
                Context context)
                throws IOException, InterruptedException {
        	System.out.println("#### REDUCE ####");
//        	System.out.println(key +" "+values);
//        	System.out.println(Bytes.toString(key.getBytes()).replace(";"," "));
        	System.out.println(key.toString());
//        	System.out.println(values.iterator().next());
        	
        	
        	
        	String key_concated = Bytes.toString(key.getBytes());            
            String course_name = key_concated.split("/")[1];
            
        	int count = 0;
        	float sum = 0;
        	for (FloatWritable val : values) {
              sum += val.get();
              count++;
          }
        	System.out.println("key:'"+key_concated.split("/")[0]+ "' course name: '"+course_name.replace(";"," ")+"' rate: '"+sum/count+"'");
        	System.out.println("$$$$ PUT $$$$");
        	Put put = new Put(key_concated.split("/")[0].getBytes());
            put.addImmutable(Bytes.toBytes("#"), Bytes.toBytes("NAME"), Bytes.toBytes(course_name.replace(";"," ")));
            put.addImmutable(Bytes.toBytes("#"), Bytes.toBytes("RATE"), Bytes.toBytes(Float.toString(sum/count)));

            context.write(null,put);
        	
        }
    }
	
	//MAIN 
	public static void main(String[] args) throws Exception {
		System.out.println("################# QUESTION 3 - START #################");
		Configuration conf = HBaseConfiguration.create();		
		Job job = Job.getInstance(conf,"question3_job");

		
	    Connection connection = ConnectionFactory.createConnection(conf);

				
		//Rules to Students
		Table tableCourse = connection.getTable(TableName.valueOf(tableC));
		//Create Table
				TableName tableNameQ3 = TableName.valueOf("A_21805893:Q3");
				Admin hba = connection.getAdmin();
				HTableDescriptor tableDescriptor = new HTableDescriptor(tableNameQ3);
				tableDescriptor.addFamily(new HColumnDescriptor("#"));		    	
				
			    if (hba.tableExists(tableNameQ3) == true) {	    	
			    	hba.disableTable(tableNameQ3);
		    		System.out.println("Table disable "+ tableNameQ3);
		    		hba.deleteTable(tableNameQ3);
		    		System.out.println("Table delete "+ tableNameQ3);		
			    	
			    }else { 	
			    		
				        hba.createTable(tableDescriptor);
				        System.out.println("Table created "+ tableNameQ3);
				        }
			    
			    if (hba.tableExists(tableNameQ3) == false) {
					hba.createTable(tableDescriptor);
					System.out.println("Table created "+ tableNameQ3);
			    }

			    
		//"S01A001/7984".getBytes(),"S01A010/7984".getBytes()
        Scan scanCourse = new Scan();
        
        job.setJarByClass(Question3.class);
        scanCourse.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scanCourse.setCacheBlocks(false);  // don't set to true for MR jobs
        System.out.println("############# call Map With ################");
//        System.out.println("Getting Course "+id+" at Table name " + tableCourse.getName());
        
        TableMapReduceUtil.initTableMapperJob(
        		tableCourse.getName(), // input HBase table name
        		scanCourse,// Scan instance to control CF and attribute selection
        		TokenizerMapper3.class,// mapper
        		Text.class, // mapper output key
        		FloatWritable.class, // mapper output value
        		job);
        //job.setCombinerClass(Reducer3.class);
        TableMapReduceUtil.initTableReducerJob(
        		"A_21805893:Q3",      // output table
        		Reducer3.class,             // reducer class
        		job); 
        


//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(FloatWritable.class);
		//job.setOutputFormatClass(NullOutputFormat.class); // because we aren't emitting anything from mapper
        
        FileOutputFormat.getOutputPath(job);
        FileOutputFormat.setOutputPath(job, new Path("file:///localhost:9000/home/hadoop/out"));
	    
//        TableMapReduceUtil.initTableReducerJob(
//        		id,
//        		Reducer2.class,        		
//        		job);
        //TableMapReduceUtil.initTableReducerJob(tableStudent, Reducer1.class, job);

        
      //System.exit(job.waitForCompletion(true) ? 0 : 1);
      		boolean b = job.waitForCompletion(true);
      		if (!b) {
      		  throw new IOException("error with job!");
      		}
    }
}


//Pour savoir le taux de réussite d’une UE depuis sa création, par rapport, au cas où, à ses
//différents noms.
///Aiwsbu/v1/courses/{id}/rates
//Le serveur retournera une chaîne JSON contenant simplement un nombre, par exemple
//(0.88 pour 88%) :
//[{"Name":"HPC","Rate":"0.183"},{"Name":"Big Data","Rate":"0.88"},...]
//Si l’UE n’existe pas, le serveur retournera une erreur NOT FOUND