package com.chinasofti.sort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupingComparator implements
		RawComparator<CustomerSortKey> {

	/*
	 * 基本分组规则：按第一列firstNum进行分组
	 */
	@Override
	public int compare(CustomerSortKey key1, CustomerSortKey key2) {
		return (int) (key1.customerKey - key2.customerKey);
	}

	/*
	 * @param b1 表示第一个参与比较的字节数组
	 * 
	 * @param s1 表示第一个参与比较的字节数组的起始位置
	 * 
	 * @param l1 表示第一个参与比较的字节数组的偏移量
	 * 
	 * @param b2 表示第二个参与比较的字节数组
	 * 
	 * @param s2 表示第二个参与比较的字节数组的起始位置
	 * 
	 * @param l2 表示第二个参与比较的字节数组的偏移量
	 */
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return WritableComparator.compareBytes(b1, s1, 8, b2, s2, 8);
	}

}