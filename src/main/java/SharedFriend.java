import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * Created by ca on 2020/4/16.
 */
public class SharedFriend {


    /*
	 第一阶段的map函数主要完成以下任务
	 1.遍历原始文件中每行<所有朋友>信息
	 2.遍历“朋友”集合，以每个“朋友”为键，原来的“人”为值  即输出<朋友,人>
	 */
    static class SharedFriendMapper01 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] person_friends = line.split(":");
            String person = person_friends[0];
            String[] friends = person_friends[1].split(" ");

            for(String friend : friends){
                context.write(new Text(friend), new Text(person));
            }
        }
    }

    /*
      第一阶段的reduce函数主要完成以下任务
      1.对所有传过来的<朋友，list(人)>进行拼接，输出<朋友,拥有这名朋友的所有人>
     */
    static class SharedFriendReducer01 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for(Text friend : values){
                sb.append(friend.toString()).append(",");
            }
            sb.deleteCharAt(sb.length()-1);
            context.write(key, new Text(sb.toString()));
        }
    }

    /*
    第二阶段的map函数主要完成以下任务
    1.将上一阶段reduce输出的<朋友,拥有这名朋友的所有人>信息中的 “拥有这名朋友的所有人”进行排序 ，以防出现B-C C-B这样的重复
    2.将 “拥有这名朋友的所有人”进行两两配对，并将配对后的字符串当做键，“朋友”当做值输出，即输出<人-人，共同朋友>
     */
    static class SharedFriendMapper02 extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] friend_persons = line.split("\t");
            String friend = friend_persons[0];
            String[] persons = friend_persons[1].split(",");
            Arrays.sort(persons); //排序

            //两两配对
            for(int i=0;i<persons.length-1;i++){
                for(int j=i+1;j<persons.length;j++){
                    context.write(new Text(persons[i]+"-"+persons[j]+":"), new Text(friend));
                }
            }
        }
    }

    /*
    第二阶段的reduce函数主要完成以下任务
    1.<人-人，list(共同朋友)> 中的“共同好友”进行拼接 最后输出<人-人，两人的所有共同好友>
     */
    static class SharedFriendReducer02 extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            StringBuffer sb = new StringBuffer();
            Set<String> set = new HashSet<String>();
            for(Text friend : values){
                if(!set.contains(friend.toString()))
                    set.add(friend.toString());
            }
            for(String friend : set){
                sum++;
                sb.append(friend.toString()).append(",");
            }
            sb.deleteCharAt(sb.length()-1);
            Map<String,Integer> map = new HashMap<String,Integer>();
            map.put(key.toString(),sum);
            List<Map.Entry<String,Integer>> list = new ArrayList<Map.Entry<String,Integer>>((Collection<? extends Map.Entry<String, Integer>>) map.entrySet());
            Collections.sort(list,new Comparator<Map.Entry<String,Integer>>() {
                //升序排序
                public int compare(Map.Entry<String, Integer> o1,
                                   Map.Entry<String, Integer> o2) {
                    return o1.getValue().compareTo(o2.getValue());
                }

            });

            for(Map.Entry<String,Integer> mapping:list){
//                System.out.println(mapping.getKey()+":"+mapping.getValue());
                context.write(new Text(mapping.getKey()),new Text(mapping.getValue()+""));
            }
//            context.write(key, new Text(sb.toString()));
            context.write(key,new Text(sum+""));
        }
    }

    public static void main(String[] args)throws Exception {
        Configuration conf = new Configuration();
        System.setProperty("hadoop.home.dir", "C:\\a\\hadoop-2.7.0");
        //第一阶段
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(SharedFriend.class);
        job1.setMapperClass(SharedFriendMapper01.class);
        job1.setReducerClass(SharedFriendReducer01.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        List<String> lists = Arrays.asList("e:\\Medium Dataset.txt", "i:\\out\\output_reduce");
        FileInputFormat.setInputPaths(job1, new Path(lists.get(0)));
        FileOutputFormat.setOutputPath(job1, new Path(lists.get(1)));

        boolean res1 = job1.waitForCompletion(true);

//        //第二阶段
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(SharedFriend.class);
        job2.setMapperClass(SharedFriendMapper02.class);
        job2.setReducerClass(SharedFriendReducer02.class);
        job2.setNumReduceTasks(4);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job2, new Path(lists.get(1)));
        FileOutputFormat.setOutputPath(job2, new Path("I:\\out\\output_reduce1"));

        boolean res2 = job2.waitForCompletion(true);

//        System.exit(res1?0:1);
    }

}
