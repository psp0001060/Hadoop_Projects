package com.chinasofti.qqfriends;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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

public class QQFriends {

	public static class QQFriendsMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] ss = line.split("\t");
			context.write(new Text(ss[0]), new Text(ss[1]));
			context.write(new Text(ss[1]), new Text(ss[0]));
		}
	}



	public static class QQFriendsReducer extends
			Reducer<Text, Text, Text, Text> {
		

		public void reduce(Text key, Iterable<Text> i,
				Context context) throws IOException, InterruptedException {
			Set<String> set = new HashSet<String>();
			for (Text t : i) {
				set.add(t.toString());
			}
			
			if (set.size()>1) {
				for (Iterator j = set.iterator(); j.hasNext();) {
					String name = (String) j.next();
					
					for (Iterator k = set.iterator(); k.hasNext();) {
						String other = (String) k.next();
						if (!name.equals(other)) {
							context.write(new Text(name), new Text(other));
						}
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "qq friends");
		job.setJarByClass(QQFriends.class);
		job.setMapperClass(QQFriendsMapper.class);
//		job.setCombinerClass(QQFriendsReducer.class);
		
		job.setReducerClass(QQFriendsReducer.class);
		job.setOutputKeyClass(Text.class);		
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
