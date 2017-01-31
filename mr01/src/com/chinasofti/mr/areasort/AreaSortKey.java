package com.chinasofti.mr.areasort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class AreaSortKey implements WritableComparable<AreaSortKey> {
	public int length;
	public int width;

	public AreaSortKey() {
	}

	public AreaSortKey(int length, int width) {
		this.length = length;
		this.width = width;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(length);
		out.writeInt(width);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.length = in.readInt();
		this.width = in.readInt();
	}

	@Override
	public int compareTo(AreaSortKey other) {
		int temp = this.length * this.width;
		int another = other.length * other.width;
		if (temp - another > 0) {
			return 1;
		} else if (temp -another< 0) {
			return -1;
		}
		return temp - another;
	}

	@Override
	public boolean equals(Object obj) {
		AreaSortKey key = (AreaSortKey) obj;
		return length == key.length
				&& width == key.width;
	}

	@Override
	public int hashCode() {
		return (int) (length + width);
	}

}
