package bdma.bigdata.aiwsbu.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.kenai.jffi.Array;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;




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
	static class Mapper1 extends TableMapper<ImmutableBytesWritable, IntWritable> {

        private int numRecords = 0;
        private static final IntWritable one = new IntWritable(1);

        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
        	System.out.println("#### MAP ####");
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
        	
        	System.out.println(row);
        	System.out.println(values);
        	System.out.println();
        	System.out.println(values.getValue(Bytes.toBytes("C"), Bytes.toBytes("B")));
        	System.out.println(values.getColumnCells(Bytes.toBytes("#"),Bytes.toBytes("F")));
        	
        	
        	
        	
        	//Grade
        	// year/semesterstudent/course
        	//2015/072012000123/S07A006
    		
    		Scan scanGrades = new Scan();
    		scanGrades.addColumn(Bytes.toBytes("#"),Bytes.toBytes("G"));
    		ResultScanner scannerG = tableGrade.getScanner(scanGrades);
    		
    		ArrayList<String> grade_info = new ArrayList<String>(); 
    		
    		for (Result i = scannerG.next(); i != null; i = scannerG.next()) {
    			
    			System.out.println(i.toString().split("/")[1]);
    			String sem_stud_id = i.toString().split("/")[1];    			
    			String stud_id = sem_stud_id.substring(2,12);
    			System.out.println(stud_id);   			
    			
    			if (id.equals(stud_id)) {
    				System.out.println(stud_id);
    				grade_info.add(i.toString().split("/")[0]); //year
    				grade_info.add(sem_stud_id.substring(0,2));//semester
    				if ("01".equals(sem_stud_id.substring(0,2))) {
    					grade_info.add("First");//First semester
					}else {
						grade_info.add("Second");//Second semester
					}
    				
    				grade_info.add(stud_id);// student id
    				grade_info.add(i.toString().split("/")[2]);//course id
    				
    				System.out.println(grade_info);
    				break;
    			}
    		}
        	
        	
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
        }
    }
	
	
	// REDUCER
	//public static class Reducer1 extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable>
	public static class Reducer1 extends TableReducer<Text,Text,Text> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            Put put = new Put(key.toString().getBytes());
            put.addImmutable(Bytes.toBytes("details"), Bytes.toBytes("total"), Bytes.toBytes(sum));
            System.out.println(String.format("stats :   key : %d,  count : %d", Bytes.toInt(key.toString().getBytes()), sum));
            //context.write(key, put);
            context.write(key, put);
        }
    }
	
	//MAIN 
	public static void main(String[] args) throws Exception {
		System.out.println("################# QUESTION 1 - START #################");
		Configuration conf = HBaseConfiguration.create();
		
		Job job = Job.getInstance(conf,"question1_job");
		job.setJarByClass(Question1.class);
        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
		
	    Connection connection = ConnectionFactory.createConnection(conf);
		//input
		String id = "2018001000";// student id
		String program = "P";
		//Rules to Students
		Table tableStudent = connection.getTable(TableName.valueOf(tableS));
		
	    Get get =  new Get(Bytes.toBytes(id)); // Get row
	    get.addFamily(Bytes.toBytes("#"));
	    get.addFamily(Bytes.toBytes("C"));
	    get.addColumn(Bytes.toBytes("#"),Bytes.toBytes(program));	    
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
        
        job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper
        
//        TableMapReduceUtil.initTableReducerJob(
//        		table.getName().toString(),
//        		Reducer1.class,
//        		job);
        //TableMapReduceUtil.initTableReducerJob(tableStudent, Reducer1.class, job);
        
        FileOutputFormat.setOutputPath(job, new Path("/home/jessefilho/out.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}