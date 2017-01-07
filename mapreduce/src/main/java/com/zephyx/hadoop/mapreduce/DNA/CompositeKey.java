package com.zephyx.hadoop.mapreduce.DNA;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements Writable, WritableComparable<CompositeKey>{
	final public static int NUMBER_OF_CHROMOSOMES=25;
	final public static int MAX_POS=1000000 ; // 1 M

	public int getChrNo() {
		return chrNo;
	}
	public void setChrNo(int chrNo) {
		this.chrNo = chrNo;
	}
	public long getPosition() {
		return position;
	}
	public void setPosition(long position) {
		this.position = position;
	}
	int chrNo;
	long position;

	public CompositeKey() {
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(chrNo);
		out.writeLong(position);
		
	}
	public void readFields(DataInput in) throws IOException {
		chrNo = in.readInt();
		position = in.readLong();
	}
	public int compareTo(CompositeKey o) {
		// First compare for chromosome no
		int result = this.chrNo == o.chrNo ? 0: this.chrNo > o.chrNo? +1:-1 ;
		if(result != 0) return result;
		
		//comparing data for same chromosome, so now compare on position
		return this.position == o.position ? 0: this.position > o.position? +1:-1 ;
	}
	
	@Override
	public String toString() {
		return chrNo+","+position + "," ;
	}
}
