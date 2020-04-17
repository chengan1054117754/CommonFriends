import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;

/**
 * Created by ca on 2020/4/16.
 */
public class SencondarySortComparator extends WritableComparator {
    private static final IntWritable.Comparator INTWRITABLE_COMPARATOR = new IntWritable.Comparator();

    public SencondarySortComparator() {
        super(AccountBean.class);
    }
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            int firstL1 = WritableUtils.decodeVIntSize(b1[s1])+ readVInt(b1, s1);
            int firstL2 = WritableUtils.decodeVIntSize(b2[s2])+ readVInt(b2, s2);
            int cmp = INTWRITABLE_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            if (cmp != 0) {
                return cmp;
            }
            return INTWRITABLE_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1, b2,s2 + firstL2, l2 - firstL2);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
