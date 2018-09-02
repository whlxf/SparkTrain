package com.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class PartitionerApp {

    /**
     * Map操作：读取输入的文件
     *
     */
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

        @Override
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{

            String lines = value.toString(); //每一行是 手机品牌 销售数量
            String[] words = lines.split(" "); // 按照指定分隔符进行分割
            context.write(new Text(words[0]),new LongWritable(Long.parseLong(words[1])));
        }
    }

    /**
     * Reduce操作：归并操作
     */
    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            /*
            参数：Text key :输入==map的输出  Iterable<LongWritable> values 输出（是个集合Iterable?因为相同key的value可能是{1，1,1....}）
             */
            Long sum = 0L;
            for (LongWritable value:values){
                //求单词key出现的次数
                sum += value.get();
            }
            //最终统计结果的输出
            context.write(key,new LongWritable(sum));
        }
    }

    /**
     * 定义Partitioner：规定分组规则（map 的输出结果到哪个partition中）
     */
    public static class MyPartitioner extends Partitioner<Text,LongWritable>{
        @Override
        public int getPartition(Text key, LongWritable value, int i) {
            //i表示 第 i Partition
            if (key.toString().equals("xiaomi")){
                return 1;
            }
            if (key.toString().equals("huawei")){
                return 2;
            }
            if (key.toString().equals("iphone7")){
                return 3;
            }
            return 0;
        }
    }

    /**
     * 定义Driver：封装了MapReduce作业的所有信息
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration,"PartitionerApp");

        job.setJarByClass(PartitionerApp.class);


        FileInputFormat.setInputPaths(job,new Path(args[0]));


        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);


        job.setCombinerClass(MyReducer.class);

        //job的partitioner分区
        job.setPartitionerClass(MyPartitioner.class);
        //设置4个reduce，每个分区partitioner一个
        job.setNumReduceTasks(4);


        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath);
            System.out.println("outputPath exists,but now it has been deleted!");
        }


        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
