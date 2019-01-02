package bdma.bigdata.aiwsbu.mapreduce;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Question1 {
	
//	private static Connection connection = null;
	private static String tableStudent = "S";
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
	public static class Reducer1 extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            Put put = new Put(key.get());
            put.addImmutable(Bytes.toBytes("details"), Bytes.toBytes("total"), Bytes.toBytes(sum));
            System.out.println(String.format("stats :   key : %d,  count : %d", Bytes.toInt(key.get()), sum));
            context.write(key, put);
        }
    }
	
	//MAIN 
	public static void main(String[] args) throws Exception {
		System.out.println("#############################");
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf,"question1_job");
		job.setJarByClass(Question1.class);
        job.getConfiguration().set("txt", "S");
		job.setOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
		//Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		Connection connection = ConnectionFactory.createConnection(conf);		
		Table table = connection.getTable(TableName.valueOf(tableStudent));
		
		System.out.println("Table name " + table.getName());
		String id = "2018001000";// student id
//		String firtName = "F";  //First Name
//		String lastName = "L";  //Last Name
//		String program = "P";  //Last Semester
//		String bDayName = "B";  //birth date
//		String addrHome = "D";  //domicile address 
//		String email = "E";  //email
//		String phone = "P";  //phone
	    Get get =  new Get(Bytes.toBytes(id)); // Get row
//	    get.addColumn(Bytes.toBytes(firtName),Bytes.toBytes("#")); //Column name , qualifier
//	    get.addColumn(Bytes.toBytes(lastName),Bytes.toBytes("#")); //Column name , qualifier
//	    get.addColumn(Bytes.toBytes(program),Bytes.toBytes("#")); //Column name , qualifier
//	    get.addColumn(Bytes.toBytes(bDayName),Bytes.toBytes("C"));
//	    get.addColumn(Bytes.toBytes(addrHome),Bytes.toBytes("C"));
//	    get.addColumn(Bytes.toBytes(email),Bytes.toBytes("C"));
//	    get.addColumn(Bytes.toBytes(phone),Bytes.toBytes("C"));
//	    Result result = table.get(get);
//	    byte [] value = result.getValue(Bytes.toBytes("F"),Bytes.toBytes("#"));
//	    byte [] value1 = result.getValue(Bytes.toBytes("L"),Bytes.toBytes("#"));
//	    String name = Bytes.toString(value);
//	    String last = Bytes.toString(value1);
//	    System.out.println("############# result ################");
//	    System.out.println("name: " + name + " last: " + last);
	    
        Scan scan = new Scan(get);
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        
        TableMapReduceUtil.initTableMapperJob(
        		table.getName(),
        		scan,
        		Mapper1.class,
        		ImmutableBytesWritable.class,
        		IntWritable.class, job);
        
        TableMapReduceUtil.initTableReducerJob(null, Reducer1.class, job);
        
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
