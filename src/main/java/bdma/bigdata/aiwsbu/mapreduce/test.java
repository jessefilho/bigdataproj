package bdma.bigdata.aiwsbu.mapreduce;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * counts the number of userIDs
 * 
 * @author sujee ==at== sujee.net
 * 
 */
public class test {
	private static String tableCourse = "A_21805893:C";
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		
		
		Job job = Job.getInstance(conf,"test");
		
		job.setJarByClass(test.class);
		job.getConfiguration().set("txt", tableCourse);
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path("/home/out.text"));
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		
		System.out.print(scan);
		TableMapReduceUtil.initTableMapperJob(
				tableCourse, 
				scan, 
				Mapper1.class, 
				ImmutableBytesWritable.class,
				IntWritable.class, job);
		job.setOutputFormatClass(TextOutputFormat.class);
		//FileOutputFormat.setOutputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);



	}
	static class Mapper1 extends TableMapper<ImmutableBytesWritable, IntWritable> {

		private int numRecords = 0;
		private static final IntWritable one = new IntWritable(1);

		@Override
		public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
			// extract userKey from the compositeKey (userId + counter)
			ImmutableBytesWritable userKey = new ImmutableBytesWritable(row.get(), 0, Bytes.SIZEOF_INT);
			try {
				System.out.println(userKey+" - "+one);
				context.write(userKey, one);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
			numRecords++;
			if ((numRecords % 10000) == 0) {
				context.setStatus("mapper processed " + numRecords + " records so far");
			}
		}
	}
	
	public static class Reducer1 extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            Put put = new Put(key.get());
            //put.add(Bytes.toBytes("details"), Bytes.toBytes("total"), Bytes.toBytes(sum));
            System.out.println(String.format("stats :   key : %d,  count : %d", Bytes.toInt(key.get()), sum));
            context.write(key, put);
        }
    }


}
