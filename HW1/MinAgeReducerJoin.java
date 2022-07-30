import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import java.io.*;

public class MinAgeReducerJoin {
	public static class Map extends Mapper <LongWritable, Text, Text, Text>{
		Text user = new Text();
		Text friend = new Text();
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] input = value.toString().split("\\t");
			String userID = input[0];
			if (input.length == 1) {
				return;
			}
			
			String[] friendsList = input[1].split(",");
			user.set(userID);
			for (String fri : friendsList) {
				friend.set(fri);
				context.write(user, friend);
			}
			
		}
	
	}
	
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		static HashMap<String, String> userData;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			super.setup(context);
			Configuration config = context.getConfiguration();
			userData = new HashMap<String, String> ();
			String userDataPath = config.get("userdata");
			FileSystem fs = FileSystem.get(config);
			Path path = new Path("hdfs://"+userDataPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			
			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 10) {
					userData.put(arr[0], arr[9]);
				}
				line = br.readLine();
			}
			
			
		}
		
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int minAge = Integer.MAX_VALUE;
			
			for (Text friend : values) {
				if (userData.containsKey(friend.toString())) {
					String DOB = userData.get(friend.toString());
					Date current = new Date();
					int currMonth = current.getMonth()+1;
					int currYear = current.getYear() + 1900;
					String[] dobs = DOB.split("/");
					int currentAge = currYear - Integer.parseInt(dobs[2]);
					if(Integer.parseInt(dobs[0]) > currMonth){
						currentAge--;
					}
					else if (Integer.parseInt(dobs[0]) == currMonth) {
						int currDay = current.getDate();
						if(Integer.parseInt(dobs[1]) > currDay) {
							currentAge--;
						}
					}
					
					if (minAge > currentAge) {
						minAge = currentAge;
					}
				}
			}
			context.write(key, new Text(Integer.toString(minAge)));
		}
	}
	
	
	//Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //get all args
        if (otherArgs.length !=3) {
        	System.err.println("Usage: MaxAgeReducerJoin <mutual.txt Path> <userdata.txt path> <out>");
        	System.exit(2);
        }
        conf.set("userdata", otherArgs[1]);
        
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "maxagereducerjoin");
        job.setJarByClass(MinAgeReducerJoin.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        //set output key type
        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
