package cpm.zpx.rongyu;

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

        for (RongyuBean value : values) {

            if (ha == 0) {
                ha = value.getHumidity();
                ta = value.getTemper();
                ft=value.getFirsttime();
                lt=value.getLasttime();

            } else if (Math.abs(value.getTemper()-ta)<3) {
                ha=(value.getHumidity()+ha)/2;
                ta=(value.getTemper()+ta)/2;
                ft=value.getFirsttime();
                lt=lt;
                outK=key;
                outV.setFirsttime(ft);
                outV.setLasttime(lt);
                outV.setTemper(ta);
                outV.setHumidity(ha);
            }else{
                context.write(outK,outV);
                ha = value.getHumidity();
                ta = value.getTemper();
                ft=value.getFirsttime();
                lt=value.getLasttime();
                outK=key;
                outV.setFirsttime(ft);
                outV.setLasttime(lt);
                outV.setTemper(ta);
                outV.setHumidity(ha);
            }

        }
        context.write(outK,outV);


    }


}
