import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by ca on 2020/4/16.
 */
public class SencondarySortPartition extends Partitioner<AccountBean, NullWritable> {
    @Override
    public int getPartition(AccountBean key, NullWritable nullWritable, int numPartitions) {
        return (key.getAccout().toString().split("-")[0].hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
