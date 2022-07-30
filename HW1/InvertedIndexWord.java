import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class InvertedIndexWord {
	public static class Map extends Mapper <LongWritable, Text, Text, IntWritable>{

		private static int count = 0;
        private Text word = new Text(); //type of output key
        private IntWritable res;
       
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] mydata = value.toString().split(",| ");
            for (String data:mydata) {
            	data = data.replaceAll("[^a-zA-Z0-9()/]", "");
            	if (!data.equals("")) {
            		word.set(data); //set word as each input keyword
                	 //Emit word and lineCount.
            		res = new IntWritable(count);
            		context.write(word, res);
            	}
            	
            }
            
            count++;
        }
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        private Text result = new Text();
        
      
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
           
        	
        	List<Integer> nums = new ArrayList();
        	
        	for (IntWritable val : values) {
        		nums.add(Integer.parseInt(val.toString()));
        	}
        	
        	Collections.sort(nums);
        	
        	String res = "[";
        	for (Integer num : nums) {
        		if (res.length() ==  1) {
        			res += num.toString();
        		}
        		else {
        			res += ", " + num.toString();
        		}
        	}
        	
        	res += "]";
        	result.set(res);
            context.write(key, result); //create a pair <keyword, number of occurences>
        }
	}
	
	//Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //get all args
        if (otherArgs.length !=2) {
        	System.err.println("Usage: InvertedIndexWord <in> <out>");
        	System.exit(2);
        }
        
        Job job = new Job(conf, "invertedindex");
        job.setJarByClass(InvertedIndexWord.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        //set output key type
        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(IntWritable.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
