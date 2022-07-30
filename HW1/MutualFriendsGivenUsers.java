import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MutualFriendsGivenUsers {
	static String userA = "";
	static String userB = "";
	
	public static class Map extends Mapper <LongWritable, Text, Text, Text>{
		private Text user = new Text();
		private Text friends = new Text();
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			Configuration config = context.getConfiguration();
			userA = config.get("user1");
			userB = config.get("user2");
			String[] input = value.toString().split("\t");
			String userID = input[0];
			
			if ((input.length == 2) && (userID.equals(userA) || userID.equals(userB))) {
				
				String[] fList = input[1].split(",");
				friends.set(input[1]);
				
				for (int i = 0 ; i < fList.length; ++i) {
					String userKey = "";
					if (Integer.parseInt(userID) < Integer.parseInt(fList[i])) {
						userKey = userID + "," + fList[i];
					}
					else {
						userKey = fList[i] + "," + userID;
					}
					user.set(userKey);
					context.write(user, friends);
				}
				
				
			}
			
			
		}
	
	}
	
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text res = new Text();
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
			
			
			String mutual = findIntersection(friendsList[0], friendsList[1]);
			
			if (mutual != null && mutual.length() != 0) {
				res.set(mutual);
				context.write(key, res);
			}
			
		}
	}
	
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //get all args
        if (otherArgs.length !=4) {
        	System.err.println("Usage: MutualFriendsGivenUsers <user1> <user2> <in> <out>");
        	System.exit(2);
        }
        conf.set("user1", otherArgs[0]);
        conf.set("user2", otherArgs[1]);
        Job job = new Job(conf, "mutualfriendsgivenusers");
        job.setJarByClass(MutualFriendsGivenUsers.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        //set output key type
        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        //set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
