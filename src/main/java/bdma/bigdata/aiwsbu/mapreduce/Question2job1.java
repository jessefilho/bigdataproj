package bdma.bigdata.aiwsbu.mapreduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Question2job1 {
	public static String CheckParameter(String param) throws IOException 
	{
		String exist=null;
		if (param.equals("S01") || param.equals("S02") || param.equals("S03") ||param.equals("S04") ||param.equals("S05") ||param.equals("S06")||param.equals("S07")||param.equals("S08")||param.equals("S09")||param.equals("S10"))
			exist="exist";

		return exist;
	}
	public static class Mapper extends TableMapper <Text, IntWritable>{
		public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
				throws IOException, InterruptedException {

			try {
				//passing parameters to map
				Configuration conf = context.getConfiguration();
				String param = conf.get("Semester");

				// get rowKey and convert it to string
				String inKey = new String(rowKey.get());
				// get semester from rowKey and convert it to string
				String semester =inKey.split("/")[2].substring(0,3);
				//ignore rows with semester different from input semester
				if (!param.equals(semester))
				{
					return;
				}

				// set new output key having  year and student number		   
				String oKey = param+"/"+inKey.split("/")[0]+"/"+ inKey.split("/")[1];
				System.out.println(oKey);
				// get grades as output value 
				byte[] bGrades = columns.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));
				String sGrades = new String(bGrades);
				Integer grades = new Integer(sGrades);
				context.write(new Text(oKey),new IntWritable(grades));

			} catch (RuntimeException e){
				e.printStackTrace();
			}
		}
	}


	public  static class Reducer  extends TableReducer <Text, IntWritable, ImmutableBytesWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			try {
				DecimalFormat df2 = new DecimalFormat(".##");
				double average;
				int sum = 0;
				int total = 0;
				for (IntWritable grade: values) {
					Integer intGrades = new Integer(grade.toString());
					total += 1;
					sum += intGrades;
				} 
				// Calculate average of grades for every student in every year for a given semester
				average=sum/total;
				// create hbase put with rowkey as year
				Put insHBase = new Put(key.getBytes());
				// insert average value to hbase 
				insHBase.add(Bytes.toBytes("result"), Bytes.toBytes("average"), Bytes.toBytes(average));
				// write data to Hbase table
				context.write(null, insHBase);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public static  void main(String[] args) throws Exception {
			Configuration conf = HBaseConfiguration.create();
			System.out.println("Enter a semester");
			Scanner scanner = new Scanner(System. in); 
			String input = scanner. nextLine();
			String checkParam = CheckParameter (input);
			if (checkParam!=null){
				//passing parameters

				//passing parameters
				conf.set("Semester", input);
				Scan scan = new Scan();
				Job job = Job.getInstance(conf,"JobQ1"); 
				TableName tableName = TableName.valueOf("A_21805893:G");
				Connection connection =ConnectionFactory.createConnection(conf);
				Table table =connection.getTable(tableName);
				job.setJarByClass(Question2job1.class);
				//define input hbase table
				TableMapReduceUtil.initTableMapperJob(
						tableName.getName(),
						scan,
						Mapper.class,
						Text.class,
						IntWritable.class,
						job);

				// Create output table
				HBaseAdmin admin = new HBaseAdmin(conf);
				if (admin.tableExists("A_21805893:Q2job1")) 
				{
					System.out.println("Table exist");
				}
				else 
				{
					TableName tableNameR =  TableName.valueOf("A_21805893:Q2job1");
					HTableDescriptor htd = new HTableDescriptor(tableNameR);
					HColumnDescriptor hcd = new HColumnDescriptor("result");
					htd.addFamily(hcd);
					admin.createTable(htd);
				}

				//Define output table
				TableMapReduceUtil.initTableReducerJob(
						"A_21805893:Q2job1",
						Reducer.class, 
						job);

				boolean b = job.waitForCompletion(true);
				if (!b) {
					throw new IOException("error with job!");
				}
			}
			else System.out.println("NOT FOUND");


		}
	}
}
