package com.chinasofti.sort;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class CustomerSortApp01 {

	public static final String INPUT_PATH = "hdfs://127.0.0.1:9000/input/sort";
	public static final String OUTPUT_PATH = "hdfs://127.0.0.1:9000/output/sortData";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, CustomerSortApp01.class.getSimpleName());

		// 设置输入路径
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		// 设置输入格式化
		job.setInputFormatClass(TextInputFormat.class);

		// 设置自定义map
		job.setMapperClass(MyMapper.class);
		// 设置map输出类型
		job.setMapOutputKeyClass(CustomerSortKey.class);
		job.setMapOutputValueClass(LongWritable.class);

		// 分区
		job.setPartitionerClass(HashPartitioner.class);
		// 设置reduce任务
		job.setNumReduceTasks(1);

		// 排序、分组
		// 规约
		// 设置自定义reduce类
		job.setReducerClass(MyReduce.class);
		// 设置reduce输出类型
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
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

	static class MyMapper extends
			Mapper<LongWritable, Text, CustomerSortKey, LongWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(" ");
			CustomerSortKey ck = new CustomerSortKey(Long.parseLong(split[0]),
					Long.parseLong(split[1]));
			context.write(ck, new LongWritable(Long.parseLong(split[1])));
		}
	}

	static class MyReduce extends
			Reducer<CustomerSortKey, LongWritable, LongWritable, LongWritable> {
		@Override
		protected void reduce(CustomerSortKey ck, Iterable<LongWritable> v2s,
				Context context) throws IOException, InterruptedException {
			context.write(new LongWritable(ck.customerKey), new LongWritable(
					ck.customerValue));
		}
	}
}