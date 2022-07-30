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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CommonFriends {
	public static class Map extends Mapper <LongWritable, Text, Text, Text>{
		Text user = new Text();
		Text friends = new Text();
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] input = value.toString().split("\\t");
			String userID = input[0];
			if (input.length == 1) {
				return;
			}
			
			String[] friendsList = input[1].split(",");
			int N = friendsList.length;
			for (int idx = 0; idx < N; ++idx) {
				String friend = friendsList[idx];
				if (userID.equals(friend)){
					continue;
				}
				String userKey = "";
				if (Integer.parseInt(userID) < Integer.parseInt(friend)) {
					userKey = userID + "," + friend;
				}
				else {
					userKey = friend + "," + userID;
				}
				
				String[] ff = new String[N-1];
				System.arraycopy(friendsList, 0, ff, 0, idx);
				System.arraycopy(friendsList, idx + 1, ff, idx, N - idx - 1);
				String fri = String.join(",", ff);
				friends.set(fri);
				user.set(userKey);
				context.write(user, friends);
			}
			
		}
	
	}
	
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private String findIntersection(String friend1, String friend2) {
			if (friend1 == null || friend2 == null) {
				return null;
			}
			String[] friendList1 = friend1.split(",");
			String[] friendList2 = friend2.split(",");
			
			LinkedHashSet<String> list1 = new LinkedHashSet();
			for(String f1 : friendList1) {
				list1.add(f1);
			}
			String output = "";
			for(String f2 : friendList2) {
				if (list1.contains(f2)) {
					if (output == "") {
						output = f2;
					}
					else {
						output = output + "," + f2;
					}
					
				}
			}
			
			return output;
		}
	
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String[] friendsList = new String[2];
			int index = 0;
			
			for (Text value : values) {
				friendsList[index++] = value.toString();
			}
			
			Set<Text> outputKeys = new HashSet();
			
			outputKeys.add(new Text("0,1"));
			outputKeys.add(new Text("20,28193"));
			outputKeys.add(new Text("1,29826"));
			outputKeys.add(new Text("6222,19272"));
			outputKeys.add(new Text("28041,28056"));
			
			String mutual = findIntersection(friendsList[0], friendsList[1]);
			
			if (outputKeys.contains(key)) {
				context.write(key, new Text(mutual));
			}
		}
	}
	
	
	//Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //get all args
        if (otherArgs.length !=2) {
        	System.err.println("Usage: CommonFriends <in> <out>");
        	System.exit(2);
        }
        
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "commonfriends");
        job.setJarByClass(CommonFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        //set output key type
        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
