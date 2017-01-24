package com.chinasofti.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomerSortKey implements WritableComparable<CustomerSortKey> {
	public long customerKey;
	public long customerValue;

	public CustomerSortKey() {
	}

	public CustomerSortKey(long customerKey, long customerValue) {
		this.customerKey = customerKey;
		this.customerValue = customerValue;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(customerKey);
		out.writeLong(customerValue);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.customerKey = in.readLong();
		this.customerValue = in.readLong();
	}

	@Override
	public int compareTo(CustomerSortKey other) {
		long temp = this.customerKey - other.customerKey;
		if (temp > 0) {
			return 1;
		} else if (temp < 0) {
			return -1;
		}
		return (int) (this.customerValue - other.customerValue);
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		CustomerSortKey key = (CustomerSortKey) obj;
		return customerKey == key.customerKey
				&& customerValue == key.customerValue;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return (int) (customerKey + customerValue);
	}

}
