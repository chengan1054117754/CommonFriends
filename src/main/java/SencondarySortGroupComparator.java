import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ca on 2020/4/16.
 */
public class SencondarySortGroupComparator extends WritableComparator {
    public SencondarySortGroupComparator() {
        super(AccountBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        AccountBean acc1 = (AccountBean)a;
        AccountBean acc2 = (AccountBean)b;
        return acc1.getAccout().toString().split("-")[0].compareTo(acc2.getAccout().toString().
        split("-")[0]);//账号相同的在一个组
    }

}
