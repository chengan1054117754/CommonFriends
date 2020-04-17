import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ca on 2020/4/16.
 */
public class SencondarySortReducer extends Reducer<AccountBean, NullWritable, Text, Text> {
    @Override
    protected void reduce(AccountBean key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        int flat = 0;
        for (NullWritable nullWritable : values) {
            if (flat<10000){
                sb.append(key.getAccout().toString().split("-")[1]).append(",");
                flat++;
            }else {
                break;
            }
//            String substring = str.substring(0, -1);
        }
        System.err.println(flat);
        sb.deleteCharAt(sb.length()-1);
        context.write(new Text(key.toString().split("-")[0]), new Text(sb.toString()));
    }
}



