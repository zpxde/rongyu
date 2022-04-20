package cpm.zpx.rongyu;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1.定义一个类实现writable接口
 * 2.重写序列化和反序列化方法
 * 3.重写空参构造
 * 4.tostring方法
 */
public class RongyuBean implements WritableComparable<RongyuBean>{

    private long firsttime;
    private long lasttime;
    private long temper;
    private long humidity;
    //空参构造
    public RongyuBean() {
    }

    public long getFirsttime() {
        return firsttime;
    }

    public void setFirsttime(long firsttime) {
        this.firsttime = firsttime;
    }

    public long getLasttime() {
        return lasttime;
    }

    public void setLasttime(long lasttime) {
        this.lasttime = lasttime;
    }

    public long getTemper() {
        return temper;
    }

    public void setTemper(long temper) {
        this.temper = temper;
    }

    public long getHumidity() {
        return humidity;
    }

    public void setHumidity(long humidity) {
        this.humidity = humidity;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(firsttime);
        out.writeLong(lasttime);
        out.writeLong(temper);
        out.writeLong(humidity);
    }


    @Override
    public void readFields(DataInput in) throws IOException {

        this.firsttime= in.readLong();
        this.lasttime = in.readLong();
        this.temper=in.readLong();
        this.humidity = in.readLong();

    }

    @Override
    public String toString() {
        return
                  firsttime + "\t"+lasttime  + "\t"+temper+"\t"+humidity  ;
    }

    @Override
    public int compareTo(RongyuBean o) {
        if (this.firsttime>o.firsttime)
            return  1;
        else if (this.firsttime < o.firsttime) {
            return -1;
        } else {
            return 0;
        }
    }
}
