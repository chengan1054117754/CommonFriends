import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created by ca on 2020/4/16.
 */
public class SencondarySortMapper extends Mapper<LongWritable, Text, AccountBean, NullWritable> {

    private AccountBean acc = new AccountBean();
    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {
//        StringTokenizer st = new StringTokenizer(value.toString());


        String[] split = StringUtils.split(value.toString(), '\t');


//        while (st.hasMoreTokens()) {
            acc.setAccout(new Text(split[0]));
            acc.setCost(new IntWritable(Integer.parseInt(split[1])));
//        }
        context.write(acc ,NullWritable.get());
    }

}
