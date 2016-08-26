package itemBasedRec;



import itemBasedRec.model.RecSort;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * 
 * 按照推荐商品分数的降序进行排序
 * 
 */
public class ItemBasedRecStep6 {

	public static final Pattern DELIMITER = Pattern.compile("[\t:]");

	public static class Step6_PartialMultiplyMapper   extends
		Mapper<LongWritable, Text, RecSort, NullWritable>{

		  protected void map(LongWritable key, Text values, 
                  Context context) throws IOException, InterruptedException {
				  String[] tokens = DELIMITER.split(values.toString());
				 int userID = Integer.valueOf(tokens[0]);
				 int itemID = Integer.valueOf(tokens[1]);
				 double sumPref= Double.valueOf(tokens[2]);
				 context.write(new RecSort(userID,itemID,sumPref), NullWritable.get());
		}
	}
	  public static class Step6Reducer extends  Reducer<RecSort, NullWritable, IntWritable, Text> {
	        private final static Text v = new Text();
	        private final static IntWritable k = new IntWritable();
	        @Override
	        protected void reduce(RecSort key, Iterable<NullWritable> values, 
			        Context context) throws IOException, InterruptedException {
	        	
	        	k.set(key.getUserID());
	        	v.set(key.getItemID()+":"+key.getSumPref());
	        	context.write(k, v);
	    }
	  }

	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException 
	{

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ItemBasedRecStep6");
		
		job.setJarByClass(ItemBasedRecStep6.class);
        String input = path.get("Step6Input");
        String output = path.get("Step6Output");
		
		
        FileInputFormat.setInputPaths(job, new Path(input) );
		FileOutputFormat.setOutputPath(job, new Path(output));
		//检查一下参数所指定的输出路径是否存在，如果已存在，先删除
		Path outPath = new Path(output);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		
	    job.setMapOutputKeyClass(RecSort.class);
	    job.setMapOutputValueClass(NullWritable.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Step6_PartialMultiplyMapper.class);
		job.setReducerClass(Step6Reducer.class);

		
		job.waitForCompletion(true);
	}

}

