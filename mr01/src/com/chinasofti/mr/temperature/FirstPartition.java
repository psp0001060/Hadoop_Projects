package com.chinasofti.mr.temperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartition extends Partitioner<KeyPair, Text> {


	@Override
	public int getPartition(KeyPair key, Text arg1, int num) {
		return (key.getYear()*127)%num;
	}

}
