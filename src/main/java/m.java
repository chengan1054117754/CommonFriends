import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ca on 2020/4/16.
 */
public class m {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.setProperty("hadoop.home.dir", "C:\\a\\hadoop-2.7.0");
        Path outfile = new Path("file:///i:/out/outtwo1");
        FileSystem fs = outfile.getFileSystem(conf);
        if(fs.exists(outfile)){
            fs.delete(outfile,true);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(m.class);
        job.setJobName("Sencondary Sort");
        job.setMapperClass(SencondarySortMapper.class);
        job.setMapOutputKeyClass(AccountBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(SencondarySortReducer1.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //声明自定义分区和分组
        job.setPartitionerClass(SencondarySortPartition.class);
        job.setGroupingComparatorClass(SencondarySortGroupComparator.class);
//        job.setSortComparatorClass(SencondarySortComparator.class);//组内排序需要声明的类

        FileInputFormat.addInputPath(job, new Path("I:\\out\\output_reduce1"));
        FileOutputFormat.setOutputPath(job,outfile);
        System.exit(job.waitForCompletion(true)?0:1);
    }





}

class SencondarySortReducer1 extends Reducer<AccountBean, NullWritable, Text, Text> {
    @Override
    protected void reduce(AccountBean key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
        boolean funcation = funcation(key.getAccout().toString().split("-")[0]);
        if (funcation){
            int flat = 0;
            List<String> list = new ArrayList<String>();
            for (NullWritable nullWritable : values) {

                list.add(key.getAccout()+"\t"+key.getCost());

//            context.write(key, NullWritable.get());
            }

            for (int i=0;i<list.size();i++)
            {
                for (int j=i+1;j<list.size();j++)
                {
                    if(list.get(i).equals(list.get(j))){
                        list.remove(j);
                    }
                }
            }
            StringBuffer sb = new StringBuffer();
            for(String str : list){
                if (flat<10){
                    sb.append(str.split("-")[1].split(":")[0]).append(",");
                    flat++;
                }else {
                    break;
                }

            }

            sb.deleteCharAt(sb.length()-1);
            context.write(new Text(key.getAccout().toString().split("-")[0]), new Text(sb.toString()));
        }


    }

    public static boolean funcation(String str){
        str = str.trim();

        if (str.length()>5){

            String substring = str.substring(str.length() - 5);
            if (substring.equals("13233")){
                return true;
            }
            return false;
        }
        return false;
    }
}

