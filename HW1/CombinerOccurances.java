import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.*;

public class CombinerOccurances extends Configured implements Tool{
	
	private final static IntWritable one = new IntWritable(1);
	private static Text word = new Text();
    
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			
			String[] inputs = values.toString().split(",|\\t");
			word.set(inputs[0]);
			for (String inp : inputs) {
				context.write(word, one);
			}
		}	
			
	}
		
		public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	        List<String> maxElements = new ArrayList<String>();
	        private static IntWritable maxSize = new IntWritable();
	      
	        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
	           int count = 0;
	           
	           for (IntWritable val : values) {
	        	   count += val.get();
	           }
	           
	           if (count > maxSize.get()) {
	        	   maxSize.set(count);
	        	   maxElements.clear();
	        	   maxElements.add(key.toString());
	           }
	           else if (count == maxSize.get()) {
	        	   maxElements.add(key.toString());
	           }
	        	
	        }
	        
	        public void cleanup(Context context) throws IOException, InterruptedException{
	        	for (String ele : maxElements) {
	        		context.write(new Text(ele), maxSize);
	        	}
	        }
		}
	
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new CombinerOccurances(), args);
		System.exit(res);
	}
	
	public int run(String[] otherArgs) throws Exception{
		Configuration conf = new Configuration();
        //get all args
        if (otherArgs.length != 2 ) {
        	System.err.println("Usage: CombinerOccurances <userdataInput>  <outputLocation>");
        	System.exit(2);
        }
        
        Job job = new Job(conf, "inverted_index_word");
        job.setJarByClass(CombinerOccurances.class);
        job.setMapperClass(InvertedIndexWord.Map.class);
        job.setReducerClass(InvertedIndexWord.Reduce.class);
        
        String tmpPath = "/tmp/maxTmp/";
        //set output key type
        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(IntWritable.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(tmpPath));
        //Wait till job completion
        int code = job.waitForCompletion(true) ? 0 : 1;
        
        Configuration conf2 = getConf();
        Job job2 = new Job(conf2, "max_occurance");
        job2.setJarByClass(CombinerOccurances.class);
        job2.setMapperClass(Map.class);
        job2.setCombinerClass(Reduce.class);
        job2.setReducerClass(Reduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
    
        
        FileInputFormat.addInputPath(job2, new Path(tmpPath));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
        code = job2.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
        return code;
	}
	
	
}
