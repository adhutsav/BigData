import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.*;

public class InMemoryMapper extends Configured implements Tool{
	static Set<String>commonFriends;
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private Text user = new Text();
		private Text dobList = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			Configuration config = context.getConfiguration();
			String[] common = value.toString().split("\t");
			//Read the common Friends and store that in the hash map. 
			if (common.length == 2 && common[1].length() != 0) {
				commonFriends = new HashSet();
				String[] cfList = common[1].split(",");
				for (String cf : cfList) {
					commonFriends.add(cf);
				}
			}
			
			String userDataPath = config.get("userdata");
			FileSystem fs = FileSystem.get(config);
			Path path = new Path("hdfs://"+userDataPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String lineInput = br.readLine();
			int count = 0;
			StringBuilder res = new StringBuilder("[");
			while(lineInput != null) {
				String[] input = lineInput.split(",");
				if (commonFriends.contains(input[0])) {
					String dob = input[9];
					String[] ddmmyyyy = dob.split("/");
					if (Integer.parseInt(ddmmyyyy[2]) >= 1995) {
						count++;
					}
					if (res.length() == 1) {
						res.append(dob);
					}
					else {
						res.append(", " + dob);
					}
					
				}
				lineInput = br.readLine();
				
			}
			res.append("], ");
			res.append(Integer.toString(count));
			user.set(common[0]);
			dobList.set(res.toString());
			context.write(user, dobList);
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new InMemoryMapper(), args);
		System.exit(res);
	}
	
	public int run(String[] otherArgs) throws Exception{
		Configuration conf = new Configuration();
        //get all args
        if (otherArgs.length !=6 ) {
        	System.err.println("Usage: InMemoryMapper <user1> <user2> <in> <out> <userdataIn> <userdataOut>");
        	System.exit(2);
        }
        conf.set("user1", otherArgs[0]);
        conf.set("user2", otherArgs[1]);
        Job job = new Job(conf, "mutualfriendsgivenusers");
        job.setJarByClass(InMemoryMapper.class);
        job.setMapperClass(MutualFriendsGivenUsers.Map.class);
        job.setReducerClass(MutualFriendsGivenUsers.Reduce.class);
        
        //set output key type
        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        //set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        //Wait till job completion
        int code = job.waitForCompletion(true) ? 0 : 1;
        
        System.err.println("Done with creation of the common Friends !!");
        
        Configuration conf2 = getConf();
        conf2.set("userdata", otherArgs[4]);
        Job job2 = new Job(conf2, "InMemoryMapper");
        job2.setJarByClass(InMemoryMapper.class);
        job2.setMapperClass(Map.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2, new Path(otherArgs[3]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[5]));
        code = job2.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
        return code;
	}
	
}
