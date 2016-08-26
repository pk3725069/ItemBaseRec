package itemBasedRec;



import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * 
 * 整理数据,相同的数据相加
 * 
 */
public class ItemBasedRecStep5 {

	public static final Pattern DELIMITER = Pattern.compile("[\t:]");

	public static class Step5_PartialMultiplyMapper   extends
		Mapper<LongWritable, Text, Text, DoubleWritable>{
        private final static Text k = new Text();
        private final static DoubleWritable v = new DoubleWritable();

		  protected void map(LongWritable key, Text values, 
                  Context context) throws IOException, InterruptedException {
				  String[] tokens = DELIMITER.split(values.toString());
				  k.set(tokens[0]+","+tokens[1]);
				  v.set(Double.valueOf(tokens[2]));
				  context.write(k, v);
		}
	}
	  public static class Step5Reducer extends  Reducer<Text, DoubleWritable, IntWritable, Text> {
	        private final static Text v = new Text();
	        private final static IntWritable k = new IntWritable();
	        @Override
	        protected void reduce(Text key, Iterable<DoubleWritable> values, 
			        Context context) throws IOException, InterruptedException {
	        	double sum=0;
	        	for(DoubleWritable value:values)
	        	{
	        		sum += value.get();
	        	}
	        	String [] line=key.toString().split(",");
	        	k.set(Integer.valueOf(line[0]));
	        	v.set(line[1]+":"+sum);
	        	context.write(k, v);
	    }
	  }

	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException 
	{

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ItemBasedRecStep5");
		
		job.setJarByClass(ItemBasedRecStep5.class);
        String input = path.get("Step5Input");
        String output = path.get("Step5Output");
		
		
        FileInputFormat.setInputPaths(job, new Path(input) );
		FileOutputFormat.setOutputPath(job, new Path(output));
		//检查一下参数所指定的输出路径是否存在，如果已存在，先删除
		Path outPath = new Path(output);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(DoubleWritable.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Step5_PartialMultiplyMapper.class);
		job.setReducerClass(Step5Reducer.class);

		
		job.waitForCompletion(true);
	}

}

