package bdma.bigdata.aiwsbu.mapreduce;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;


public class Qone {

	public static void main(String[] args) throws Exception {
		System.out.println("################# START #################");
		Configuration conf = HBaseConfiguration.create();
		JobClient my_client = new JobClient();
		// Create a configuration object for the job
		JobConf job_conf = new JobConf(Qone.class);
		// Set a name of the Job
		job_conf.setJobName("Question1");

		// Specify data type of output key and value
		job_conf.setOutputKeyClass(Text.class);
		job_conf.setOutputValueClass(IntWritable.class);
		
		// Specify names of Mapper and Reducer Class
		//job_conf.setMapperClass(SalesCountry.SalesMapper.class);
		//job_conf.setReducerClass(SalesCountry.SalesCountryReducer.class);

		// Specify formats of the data type of Input and output
		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);
	}
}
