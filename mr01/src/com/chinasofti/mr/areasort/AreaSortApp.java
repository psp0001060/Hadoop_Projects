package com.chinasofti.mr.areasort;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AreaSortApp {

	public static final String INPUT_PATH = "hdfs://127.0.0.1:9000/input/areasort";
	public static final String OUTPUT_PATH = "hdfs://127.0.0.1:9000/output/areasort";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, AreaSortApp.class.getSimpleName());

		// 设置输入路径
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		// 设置输入格式化
		job.setInputFormatClass(TextInputFormat.class);

		// 设置自定义map
		job.setMapperClass(MyMapper.class);
		// 设置map输出类型
		job.setMapOutputKeyClass(AreaSortKey.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 分区
		job.setPartitionerClass(AreaSortPartitioner.class);
		// 设置reduce任务
		job.setNumReduceTasks(2);

		// 排序、分组
		// 规约
		// 设置自定义reduce类
		job.setReducerClass(MyReduce.class);
		// 设置reduce输出类型
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		// 删除已存在的路径
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		Path path = new Path(OUTPUT_PATH);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
		// 设置输出路径
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		// 设置输出格式化类
		job.setOutputFormatClass(TextOutputFormat.class);
		// 提交任务
		job.waitForCompletion(true);
	}

	public static class AreaSortPartitioner extends
			Partitioner<AreaSortKey, IntWritable> {

		@Override
		public int getPartition(AreaSortKey ck, IntWritable width, int num) {
			//长款相等，即正方形
			if (ck.length == ck.width) {
				return 0;
			} else {
				return 1;
			}
		}
	}
	
	static class MyMapper extends
			Mapper<LongWritable, Text, AreaSortKey, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			AreaSortKey ck = new AreaSortKey(Integer.parseInt((split[0])),
					Integer.parseInt(split[1]));
			context.write(ck, new IntWritable(Integer.parseInt(split[1])));
		}
	}

	static class MyReduce extends
			Reducer<AreaSortKey, IntWritable, IntWritable, IntWritable> {
		@Override
		protected void reduce(AreaSortKey ck, Iterable<IntWritable> v2s,
				Context context) throws IOException, InterruptedException {
			context.write(new IntWritable(ck.length), new IntWritable(
					ck.width));
		}
	}
}