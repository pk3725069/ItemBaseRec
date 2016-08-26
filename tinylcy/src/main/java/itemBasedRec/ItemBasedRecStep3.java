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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * 计算同现矩阵：将Step1的输出作为Step2的输入
 * 合并同现矩阵和评分矩阵
 * 
 */
public class ItemBasedRecStep3 {

	public static final Pattern DELIMITER = Pattern.compile("[\t,]");

	private final static Text v = new Text();
    private final static IntWritable k = new IntWritable();
    //input 用户  物品:评分, output 物品   用户：评分
	public static class Step31_UserVectorSplitterMapper  extends
		Mapper<LongWritable, Text, IntWritable, Text>{
		  protected void map(LongWritable key, Text values, 
                  Context context) throws IOException, InterruptedException {
			String[] tokens = DELIMITER.split(values.toString());
			for (int i = 1; i < tokens.length; i++) {
                String[] vector = tokens[i].split(":");
                int itemID = Integer.parseInt(vector[0]);
                String pref = vector[1];

                k.set(itemID);
                v.set(tokens[0] + ":" + pref);
                context.write(k, v);
            }
		  }
	}
	public static void run1(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ItemBasedRecStep31");
		
		job.setJarByClass(ItemBasedRecStep3.class);
		String input = path.get("Step3Input1");
	    String output = path.get("Step3Output2");
		
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		//检查一下参数所指定的输出路径是否存在，如果已存在，先删除
		Path outPath = new Path(output);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Step31_UserVectorSplitterMapper.class);

		
		job.waitForCompletion(true);
    }
	//input 物品：物品  点赞次数  output 物品：物品  点赞次数 
	public static class Step32_CooccurrenceColumnWrapperMapper  extends Mapper<LongWritable, Text, Text, IntWritable>  {
	    private final static Text k = new Text();
        private final static IntWritable v = new IntWritable();
        @Override
        protected void map(LongWritable key, Text values, 
                Context context) throws IOException, InterruptedException {
            String[] tokens = DELIMITER.split(values.toString());
            k.set(tokens[0]);
            v.set(Integer.parseInt(tokens[1]));
            context.write(k, v);
        }		
	}


	public static void run2(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException 
	{

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ItemBasedRecStep32");
		
		job.setJarByClass(ItemBasedRecStep3.class);
		String input = path.get("Step3Input2");
	    String output = path.get("Step3Output1");
		
		
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
		
		job.setMapperClass(Step32_CooccurrenceColumnWrapperMapper.class);

		job.waitForCompletion(true);
	}

}
