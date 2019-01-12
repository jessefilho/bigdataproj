package bdma.bigdata.aiwsbu.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
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
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bdma.bigdata.aiwsbu.Namespace;
import bdma.bigdata.aiwsbu.data.Setup;
import bdma.bigdata.aiwsbu.mapreduce.WordCount.IntSumReducer;
import bdma.bigdata.aiwsbu.mapreduce.WordCount.TokenizerMapper;


/**
 * counts the number of userIDs
 * 
 * @author sujee ==at== sujee.net
 * 
 */
public class test {
	private static String tableCourse = "C";


	private static Connection connection = null;


	public static void main(String[] args) throws Exception {
		//Setup setup = new Setup();
		Configuration conf = HBaseConfiguration.create();		
		Job job = Job.getInstance(conf,"test_jobName");
		Table table = connection.getTable(TableName.valueOf(tableCourse));  
		
		System.out.println(table.getName());
//		Scan scan = new Scan();
//		//ResultScanner fromscanner = table.getScanner(scan);
//		job.setJarByClass(test.class);
//		job.getConfiguration().set("txt", tableCourse);
//		job.setMapperClass(TokenizerMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		
//		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

		//scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		//scan.setCacheBlocks(false);  // don't set to true for MR jobs
		// Scanning the required columns
		//scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));


		//System.out.print(scan);
		//		TableMapReduceUtil.initTableMapperJob(
		//				tableCourse, 
		//				scan, 
		//				Mapper1.class, 
		//				ImmutableBytesWritable.class,
		//				IntWritable.class, job);
		//		job.setOutputFormatClass(TextOutputFormat.class);

		//put here the mapred output to input file



	}

	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
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
