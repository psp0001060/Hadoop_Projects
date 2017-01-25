package com.chinasofti.mr.temperature;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

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

public class FindTemperatureTime {

	public static final String INPUT_PATH = "hdfs://127.0.0.1:9000/input/temperature";
	public static final String OUTPUT_PATH = "hdfs://127.0.0.1:9000/output/temperData";

	public static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, FindTemperatureTime.class.getSimpleName());

		// 设置输入路径
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		// 设置输入格式化
		job.setInputFormatClass(TextInputFormat.class);

		// 设置自定义map
		job.setMapperClass(HotMapper.class);
		// 设置map输出类型
		job.setMapOutputKeyClass(KeyPair.class);
		job.setMapOutputValueClass(Text.class);

		// 分区
		job.setPartitionerClass(FirstPartition.class);
		// 设置reduce任务
		job.setNumReduceTasks(3);
		

		// 排序、分组
		job.setSortComparatorClass(SortHot.class);
		job.setGroupingComparatorClass(GroupHot.class);
		// 规约
		// 设置自定义reduce类
		job.setReducerClass(HotReduce.class);
		// 设置reduce输出类型
		job.setOutputKeyClass(KeyPair.class);
		job.setOutputValueClass(Text.class);
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

	static class HotMapper extends
			Mapper<LongWritable, Text, KeyPair, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] ss = line.split("\t");
			if (ss.length ==2) {
				try {
					Date date = SDF.parse(ss[0]);
					Calendar c = Calendar.getInstance();
					c.setTime(date);
					int year = c.get(1);
					String hot=ss[1].substring(0, ss[1].indexOf("℃"));
					KeyPair kp = new KeyPair();
					kp.setYear(year);
					kp.setHot(Integer.parseInt(hot));
					context.write(kp, value);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
	}

	static class HotReduce extends
			Reducer<KeyPair, Text, KeyPair, Text> {
		@Override
		protected void reduce(KeyPair kp, Iterable<Text> value,
				Context context) throws IOException, InterruptedException {
			for (Text v : value) {
				context.write(kp, v);
			}
		}
	}
}