package itemBasedRec;


import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * 按用户分组，计算所有物品出现的组合列表，得到用户对物品的评分矩阵
 * 比如 value为 1,101,5；1,102，3 --> 1 101:5,102:3
 */
public class ItemBasedRecStep1 {

	public static final Pattern DELIMITER = Pattern.compile("[\t,]");

	public static class Step1MapClass extends Mapper<LongWritable,Text, IntWritable, Text>{
		private IntWritable k = new IntWritable();
		private Text v = new Text();
		  protected void map(LongWritable key, Text values, 
	                  Context context) throws IOException, InterruptedException {
			   String[] tokens = DELIMITER.split(values.toString());
			  int userID = Integer.parseInt(tokens[0]);
	            String itemID = tokens[1];
	            String pref = tokens[2];
	            k.set(userID);
	            v.set(itemID + ":" + pref);
	            context.write(k, v);
		}
	}
	


	public static class Step1ReduceClass extends 
			Reducer<IntWritable, Text, IntWritable, Text> {

		private Text v = new Text();

		public void reduce(IntWritable key, Iterable<Text> values, 
                Context context)
				throws IOException, InterruptedException {

			StringBuilder sBuilder = new StringBuilder();
			for(Text value : values) {
				sBuilder.append("," +value );
			}
			v.set(sBuilder.toString().replaceFirst(",", ""));
			 context.write(key, v);

		}

	}

	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ItemBasedRecStep1");
		
		job.setJarByClass(ItemBasedRecStep1.class);
		String input = path.get("Step1Input");
	    String output = path.get("Step1Output");
		//检查一下参数所指定的输出路径是否存在，如果已存在，先删除
		Path outPath = new Path(output);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step1MapClass.class);
		job.setReducerClass(Step1ReduceClass.class);
		job.waitForCompletion(true);
	//	System.exit( ? 0 : 1);
	}
}
