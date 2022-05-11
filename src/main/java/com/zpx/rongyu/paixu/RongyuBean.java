package com.zpx.rongyu.paixu;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RongyuBean
        implements WritableComparable<RongyuBean>
{
    private long firsttime;
    private long lasttime;
    private long temper;
    private long humidity;

    public long getFirsttime()
    {
        return this.firsttime;
    }

    public void setFirsttime(long firsttime) {
        this.firsttime = firsttime;
    }

    public long getLasttime() {
        return this.lasttime;
    }

    public void setLasttime(long lasttime) {
        this.lasttime = lasttime;
    }

    public long getTemper() {
        return this.temper;
    }

    public void setTemper(long temper) {
        this.temper = temper;
    }

    public long getHumidity() {
        return this.humidity;
    }

    public void setHumidity(long humidity) {
        this.humidity = humidity;
    }

    public void write(DataOutput out)
            throws IOException
    {
        out.writeLong(this.firsttime);
        out.writeLong(this.lasttime);
        out.writeLong(this.temper);
        out.writeLong(this.humidity);
    }

    public void readFields(DataInput in)
            throws IOException
    {
        this.firsttime = in.readLong();
        this.lasttime = in.readLong();
        this.temper = in.readLong();
        this.humidity = in.readLong();
    }

    public String toString()
    {
        return this.firsttime + "\t" + this.lasttime + "\t" + this.temper + "\t" + this.humidity;
    }

    public int compareTo(RongyuBean o)
    {
        if (this.firsttime > o.firsttime)
            return 1;
        if (this.firsttime < o.firsttime) {
            return -1;
        }
        return 0;
    }
}
