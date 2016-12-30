package com.elixir.hadoop.mapreduce.numeric.summarization.minmaxcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MinMaxCountTuple implements Writable
{

  public MinMaxCountTuple()
  {
  }

  public MinMaxCountTuple(long parseLong, long parseLong2, Long i)
  {
    setMinPostTime(parseLong);
    setMaxPostTime(parseLong2);
    setCount(i);
  }

  public long getMinPostTime()
  {
    return minPostTime;
  }

  public void setMinPostTime(long minPostTime)
  {
    this.minPostTime = minPostTime;
  }

  public long getMaxPostTime()
  {
    return maxPostTime;
  }

  public void setMaxPostTime(long maxPostTime)
  {
    this.maxPostTime = maxPostTime;
  }

  public long getCount()
  {
    return count;
  }

  public void setCount(long count)
  {
    this.count = count;
  }

  long minPostTime;
  long maxPostTime;
  long count;

  public void write(DataOutput out) throws IOException
  {
    out.writeLong(minPostTime);
    out.writeLong(maxPostTime);
    out.writeLong(count);

  }

  public void readFields(DataInput in) throws IOException
  {
    minPostTime = in.readLong();
    maxPostTime = in.readLong();
    count = in.readLong();
  }

  @Override
  public String toString()
  {
    return minPostTime + " " + maxPostTime + " " + count;
  }

}
