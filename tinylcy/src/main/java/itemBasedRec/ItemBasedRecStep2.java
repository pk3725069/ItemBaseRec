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
 * 计算同现矩阵：将Step1的输出作为Step2的输入
 * 比如，对于一行数据：1 101:5.0，102:3.0,103:2.5
 * map输出：101:101 1,101:102 1,101:103 1,102:101 1,102:102 1,
 *         102:103 1,103:101 1，103:102 1,103:103 1
 */
public class ItemBasedRecStep2 {

	public static final Pattern DELIMITER = Pattern.compile("[\t,]");

	private final static Text k = new Text();
    private final static IntWritable v = new IntWritable(1);
    
	public static class Step2MapClass extends
		Mapper<LongWritable, Text, Text, IntWritable> {
		  protected void map(LongWritable key, Text values, 
                  Context context) throws IOException, InterruptedException {
			String[] tokens = DELIMITER.split(values.toString());
	        for (int i = 1; i < tokens.length; i++) {
	            String itemID = tokens[i].split(":")[0];
	            for (int j = 1; j < tokens.length; j++) {
	                String itemID2 = tokens[j].split(":")[0];
	                k.set(itemID + ":" + itemID2);
	                context.write(k, v);
	            }
	        }
		  }
	}

	public static class Step2ReduceClass extends Reducer<Text,IntWritable,   Text,IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, 
		        Context context)
					throws IOException, InterruptedException {
				int sum = 0;
	            for(IntWritable value : values) {
	                sum += value.get();
	            }
	            result.set(sum);
				context.write(key, result);
			}
		
	}


	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException 
	{

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ItemBasedRecStep2");
		
		job.setJarByClass(ItemBasedRecStep2.class);
		String input = path.get("Step2Input");
	    String output = path.get("Step2Output");
		
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		//检查一下参数所指定的输出路径是否存在，如果已存在，先删除
		Path outPath = new Path(output);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Step2MapClass.class);
		job.setReducerClass(Step2ReduceClass.class);
		
		job.waitForCompletion(true);
	}

}
