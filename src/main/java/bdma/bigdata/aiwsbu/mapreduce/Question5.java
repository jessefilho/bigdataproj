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
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.omg.CORBA.portable.ValueOutputStream;

import bdma.bigdata.aiwsbu.mapreduce.WordCount.IntSumReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collector;
import java.util.stream.Collectors;




public class Question5 {

	
	private static String tableG = "A_21805893:G";
	private static String tableC = "A_21805893:C";

	//MAPPER
	static class Mapper3 extends TableMapper<Text, FloatWritable> {
		private final static FloatWritable gradess = new FloatWritable();
	    private Text keyy = new Text();
        public void map(ImmutableBytesWritable row,
        		Result values,
        		Context context) throws InterruptedException, IOException {
        	System.out.println("#### MAP ####");
        	System.out.println("Processing ...");
        	
//        	System.out.println(row);
//        	System.out.println(values);
//        	
//        	System.out.println(Bytes.toString(values.getRow()));
//        	System.out.println(values.getColumnCells(Bytes.toBytes("#"),Bytes.toBytes("N")));
//        	System.out.println(values.getValue(Bytes.toBytes("#"),Bytes.toBytes("N")));
        	
        	String courseName = Bytes.toString(values.getValue(Bytes.toBytes("#"),Bytes.toBytes("N")));
        	String[] ue_course = Bytes.toString(values.getRow()).split("/");
        	List<Float> gradesList = new ArrayList<>();
        	//List<List> eachGrades = new ArrayList<>();
        	String key = null;

        	//Grades
        	// year/semesterstudent/course
        	//2015/072012000123/S07A006
    		System.out.print(" ...");
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
    		
    		
    		for (Result iC = scannerC.next(); iC != null; iC = scannerC.next()) {//Start FOR iC
    			
    			if (ue_course[0].equals(Bytes.toString(iC.getRow()).split("/")[0])) {
    				String courseNameFromScan = Bytes.toString(iC.getValue(Bytes.toBytes("#"),Bytes.toBytes("N")));
    				key = courseNameFromScan;//ue_course[0]+"/"+courseNameFromScan;    			
    				
		    		for (Result iG = scannerG.next(); iG != null; iG = scannerG.next()) {//Start FOR iG
		    			List<Cell> pointer = iG.getColumnCells(Bytes.toBytes("#"), Bytes.toBytes("G"));
		    			String ueFromGrades = Bytes.toString(iG.getRow()).split("/")[2];
		    			byte [] value_grades = iG.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));    			
		    			Float grades = Float.valueOf(Bytes.toString(value_grades))/100;
		    			
		    			if (ue_course[0].equals(ueFromGrades)) {
		    				//gradesList.add(grades);
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
//        	
//        	System.out.println(values.iterator().next());
        	
        	int count = 0;
        	float sum = 0;
        	for (FloatWritable val : values) {
              sum += val.get();
              count++;
          }        	

//        	System.out.println(sum);
//        	System.out.println(count);
//        	System.out.println("Name"+Bytes.toString(key.getBytes()).replace(";"," ")+"Rate: " +sum/count);
        	String keyy = Bytes.toString(key.getBytes()).replace(";"," ")+ " " + Float.toString(sum/count);
        	
        	context.write(keyy,null);
        	
        }
    }
	
	//MAIN 
	public static void main(String[] args) throws Exception {
		System.out.println("################# QUESTION 3 - START #################");
		Configuration conf = HBaseConfiguration.create();		
		Job job = Job.getInstance(conf,"question3_job");

		
	    Connection connection = ConnectionFactory.createConnection(conf);
		//input
		String id_code = "S10B036/2015";//"S10B036/7984";// course with id
		String[] id_coded = id_code.split("/");
		id_coded[1] = String.valueOf(9999 - Integer.valueOf(id_coded[1])); // code year		
		String id =id_coded[0]+"/"+id_coded[1];		
		
		//Rules to Students
		Table tableCourse = connection.getTable(TableName.valueOf(tableC));
		
	    Get get =  new Get(Bytes.toBytes(id)); // Get row
	    System.out.println(get);
	    get.addColumn(Bytes.toBytes("#"),Bytes.toBytes("N")); // COurse Name
	    
	    Result result = tableCourse.get(get);
	    byte [] value = result.getValue(Bytes.toBytes("#"),Bytes.toBytes("N"));
	    //byte [] value1 = );
	    String course_name = Bytes.toString(value);
	    //String last = Bytes.toString(value1);
	    
	    System.out.println(course_name);
        Scan scanCourse = new Scan(get);
        scanCourse.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scanCourse.setCacheBlocks(false);  // don't set to true for MR jobs
        System.out.println("############# call Map With ################");
        System.out.println("Getting Course "+id+" at Table name " + tableCourse.getName());
       
        TableMapReduceUtil.initTableMapperJob(
        		tableCourse.getName(), // input HBase table name
        		scanCourse,// Scan instance to control CF and attribute selection
        		Mapper3.class,// mapper
        		Text.class, // mapper output key
        		FloatWritable.class, // mapper output value
        		job);
        
        job.setJarByClass(Question5.class);
        job.setCombinerClass(Reducer3.class);
        job.setReducerClass(Reducer3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
		//job.setOutputFormatClass(NullOutputFormat.class); // because we aren't emitting anything from mapper
        
        FileOutputFormat.getOutputPath(job);
        FileOutputFormat.setOutputPath(job, new Path("file:///localhost:9000/home/hadoop/out"));
	    
//        TableMapReduceUtil.initTableReducerJob(
//        		id,
//        		Reducer2.class,        		
//        		job);
        //TableMapReduceUtil.initTableReducerJob(tableStudent, Reducer1.class, job);

        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


//Pour savoir le taux de réussite d’une UE depuis sa création, par rapport, au cas où, à ses
//différents noms.
///Aiwsbu/v1/courses/{id}/rates
//Le serveur retournera une chaîne JSON contenant simplement un nombre, par exemple
//(0.88 pour 88%) :
//[{"Name":"HPC","Rate":"0.183"},{"Name":"Big Data","Rate":"0.88"},...]
//Si l’UE n’existe pas, le serveur retournera une erreur NOT FOUND