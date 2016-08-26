package itemBasedRec;



import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.log4j.Logger;


/*
 * 
 * 同现矩阵和评分矩阵 相乘
 * 
 */
public class ItemBasedRecStep4 {

	public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    static  Logger logger  =  Logger.getLogger(ItemBasedRecStep4.class.getName()); 
	public static class Step4_PartialMultiplyMapper   extends
		Mapper<LongWritable, Text, IntWritable, Text>{
        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();

        //1. input 物品   用户：评分
        //2. input 物品 ：物品  评分次数 
		  protected void map(LongWritable key, Text values, 
                  Context context) throws IOException, InterruptedException {
				  String[] tokens = DELIMITER.split(values.toString());
				  	//根据字段可以区分评分矩阵还是同现矩阵
		            String[] v1 = tokens[0].split(":");
		            String[] v2 = tokens[1].split(":");
	
		            if (v1.length > 1) {// cooccurrence input 物品 ：物品  评分次数   
		                int itemID1 = Integer.parseInt(v1[0]);
		                int itemID2 = Integer.parseInt(v1[1]);
		                int num = Integer.parseInt(tokens[1]);
		                k.set(itemID1);
		                v.set("A:"+itemID2+":"+num);
			            context.write(k, v);

		            }
	
		            if (v2.length > 1) {// userVector input 物品   用户：评分
		                int itemID = Integer.parseInt(tokens[0]);
		                int userID = Integer.parseInt(v2[0]);
		                double pref = Double.parseDouble(v2[1]);
		                k.set(itemID);
		                v.set("B:"+userID+":"+pref);
		                context.write(k, v);
		            }
		}
	}
	  public static class Step4_AggregateAndRecommendReducer extends  Reducer<IntWritable, Text, IntWritable, Text> {
	        private final static Text v = new Text();
	        private final static IntWritable k = new IntWritable();
	        @Override
	        protected void reduce(IntWritable key, Iterable<Text> values, 
			        Context context) throws IOException, InterruptedException {
	        	Map<String,Integer> mapA = new HashMap<String,Integer>();
	        	Map<String,Double> mapB = new HashMap<String,Double>();
	        	for(Text value:values)
	        	{
	        		String line = value.toString();
	        		if(line.startsWith("A:"))
	        		{
	        			String [] words = line.split(":");
	        			mapA.put(words[1], Integer.valueOf(words[2]));
	        		}
	        		else if(line.startsWith("B:"))
	        		{
	        			String [] words = line.split(":");
	        			mapB.put(words[1], Double.valueOf(words[2]));
	        		}
	        	}
	        	//同现矩阵和评分矩阵 相乘
	        	for (Entry<String, Double> entryB : mapB.entrySet()) {  
		        	for (Entry<String, Integer> entryA : mapA.entrySet()) {  
		        		k.set(Integer.valueOf(entryB.getKey()));
		        		v.set(entryA.getKey()+":"+entryA.getValue()*entryB.getValue());
		        	    context.write(k, v);
		        	} 
	        	  
	        	}  
	        }
	    }

	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException 
	{

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ItemBasedRecStep4");
		
		job.setJarByClass(ItemBasedRecStep4.class);
        String input1 = path.get("Step4Input1");
        String input2 = path.get("Step4Input2");
        String output = path.get("Step4Output");
		
		
        FileInputFormat.setInputPaths(job, new Path(input1) , new Path(input2));
		FileOutputFormat.setOutputPath(job, new Path(output));
		//检查一下参数所指定的输出路径是否存在，如果已存在，先删除
		Path outPath = new Path(output);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Step4_PartialMultiplyMapper.class);
	//	job.setCombinerClass(Step4_AggregateAndRecommendReducer.class);
		job.setReducerClass(Step4_AggregateAndRecommendReducer.class);

		
		job.waitForCompletion(true);
	}

}

