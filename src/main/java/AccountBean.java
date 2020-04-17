import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ca on 2020/4/16.
 */
public class AccountBean implements WritableComparable<AccountBean> {

    private Text accout;
    private IntWritable cost;
    public AccountBean() {
        setAccout(new Text());
        setCost(new IntWritable());
    }
    public AccountBean(Text accout, IntWritable cost) {
        this.accout = accout;
        this.cost = cost;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        accout.write(out);
        cost.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        accout.readFields(in);
        cost.readFields(in);
    }
    @Override
    public int compareTo(AccountBean o) {

        int tmp = accout.toString().split("-")[0].compareTo(o.accout.toString().split("-")[0]);
        if(tmp ==0){
            return -cost.compareTo(o.cost);
        }
        return tmp;
    }
    public Text getAccout() {
        return accout;
    }

    public void setAccout(Text accout) {
        this.accout = accout;
    }

    public IntWritable getCost() {
        return cost;
    }

    public void setCost(IntWritable cost) {
        this.cost = cost;
    }
    @Override
    public String toString() {
        return accout + "\t" + cost;
    }

}
