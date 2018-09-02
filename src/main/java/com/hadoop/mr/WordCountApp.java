package com.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * 使用MapReduce实现wordcount
 */
public class WordCountApp {

    /**
     * Map操作：读取输入的文件
     *
     */
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
            /*
            key:偏移量 value：文件内容，hadoop的Text类型就相当于Java string类型  context:上下文内容
             */
            String lines = value.toString(); //接收到的每一行数据
            String[] words = lines.split(" "); // 按照指定分隔符进行分割
            for (String word: words){
                //通过上下文把map的处理结果输出,也是reduce的输入
                context.write(new Text(word),one);
            }
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
     * 定义Driver：封装了MapReduce作业的所有信息
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        //1conf
        Configuration configuration = new Configuration();
        //2创建job
        Job job = Job.getInstance(configuration,"wordcount");
        //3设置job的处理类
        job.setJarByClass(WordCountApp.class);

        //设置作业处理的输入文件路径,通过代码执行外部传入
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //设置map相关参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //优化2：通过job设置Combiner处理类，其实现逻辑与reduce是一样的
        job.setCombinerClass(MyReducer.class);

        //设置reduce相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //优化1：准备清理已存在的输出目录(为了解决多次执行，出现输出文件已存在的错误异常)
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath);
            System.out.println("outputPath exists,but now it has been deleted!");
        }

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
