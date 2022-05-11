package com.zpx.rongyu.find;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RongyuReducer extends Reducer<Text, RongyuBean, Text, RongyuBean> {
    private RongyuBean outV = new RongyuBean();
    private Text outK = new Text();


    @Override
    protected void reduce(Text key, Iterable<RongyuBean> values, Context context) throws IOException, InterruptedException {
        long ta = 0;
        long ha = 0;
        long ft = 0;
        long lt = 0;
        long nu=0;

        for (RongyuBean value : values) {

            if (ha == 0) {
                ha = value.getHumidity();
                ta = value.getTemper();
                ft = value.getFirsttime();
                lt = value.getLasttime();

                nu=value.getNumb();

                fuzhi(key, ft, lt, ta, ha,nu);
            } else if (Math.abs(value.getTemper() - ta) < 3&&Math.abs(value.getHumidity()-ha)<3) {
                ha = value.getHumidity() ;
                ta = value.getTemper();
                ft = value.getFirsttime();
                lt = lt;

                nu=value.getNumb();

                fuzhi(key, ft, lt, ta, ha,nu);
            } else {
                context.write(outK, outV);
                ha = value.getHumidity();
                ta = value.getTemper();
                ft = value.getFirsttime();
                lt = value.getLasttime();

                nu=value.getNumb();

                fuzhi(key, ft, lt, ta, ha,nu);
            }

        }

        context.write(outK, outV);

    }

    public void fuzhi(Text key, long ft, long lt, long ta, long ha,long nu) {
        outK = key;
        outV.setFirsttime(ft);
        outV.setLasttime(lt);
        outV.setTemper(ta);
        outV.setHumidity(ha);
        outV.setNumb(nu);
    }


}
