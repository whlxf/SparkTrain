package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/*
hadoop hdfs JavaApi操作
单元测试类：测试类：setUp
 */
public class HDFSApp {

    /*
    2.操作文件系统，类 FileSystem（hadoop的）是文件系统一切操作的入口，需要在setUp中创建出来的；
    配置类 Configuration，在setUp中创建
     */
    FileSystem fileSystem = null;
    Configuration configuration = null;

    /*
    3.在setUp中创建fileSystem对象需要hdfs路径
     */
    public static final String hdfs_path = "hdfs://master:8020";


    /**
     * 创建HDFS目录
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception{
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        output.write("Hello hadoop-hdfs~".getBytes()); //写入字节数组类型
        output.flush();
        output.close();
    }

    /**
     * 查看hdfs中文件的内容
     * @throws Exception
     */
    @Test
    public void cat() throws Exception{
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/a.txt")); //取到文件地址，读
        IOUtils.copyBytes(in,System.out,1024); //将in的内容控制台打印
        in.close();
    }

    /**
     * 重命名hdfs文件名称
     * @throws Exception
     */
    @Test
    public void rename() throws Exception{
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/new_a.txt");
        fileSystem.rename(oldPath,newPath); //rename返回值是Boolean型
    }

    /**
     * copy本地文件到hdfs指定位置
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception{
        Path localpath = new Path("C:\\Users\\DELL\\Desktop\\liuxf.txt");
        Path dstpath = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(localpath,dstpath);
    }

    /**
     * copy大文件到hdfs指定位置,进度条显示
     * @throws Exception
     */
    @Test
    public void copyFromLocalFileWithProgress() throws Exception{
        Path localpath = new Path("F:\\linux\\spark software\\scala-2.12.2.tgz");
        Path dstpath = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(localpath,dstpath);
        /*需要使用io的方式操作大文件，BufferedInputStream*/
        //1.确定输入,并转化格式：BufferedInputStream FileInputStream
        InputStream in = new BufferedInputStream(
                new FileInputStream(new File("F:\\linux\\spark software\\scala-2.12.2.tgz")));
        //2.确定输出,给要上传的文件创建一个名称
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/scala-2.12.2.tgz"), new Progressable() {
            @Override
            public void progress() {
                System.out.println("."); //带进度的 点
            }
        });
        //3.copy：将in 拷贝到 output
        IOUtils.copyBytes(in,output,4096);
    }

    /**
     * 下载hdfs文件
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception{

        Path localpath = new Path("C:\\Users\\DELL\\Desktop\\liuxf1.txt");
        Path sourcepath = new Path("/hdfsapi/test/liuxf.txt");
        fileSystem.copyToLocalFile(sourcepath,localpath);
    }


    /**
     * 显示某个目录下的所有文件
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception{
        FileStatus[] fileStatuses =  fileSystem.listStatus(new Path("/hdfsapi/test"));
        //迭代
        for(FileStatus fileStatus:fileStatuses){
            //判断目录下是文件还是文件夹
           String isDir = fileStatus.isDirectory() ? "文件夹":"文件";
           //获取文件副本有多少
            short replication = fileStatus.getReplication();
            //文件大小
            long len = fileStatus.getLen();
            //全路径
            String path = fileStatus.getPath().toString();

            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    /**
     * 删除hdfs文件
     * @throws Exception
     */
    @Test
    public void delete() throws Exception{
        fileSystem.delete(new Path("/hdfs"),true); //是否采用递归式删除
    }


    /*1.
    java或者使用Junit进行单元测试用例的时候，必须的两个内容是：
    setUp---准备工作；在单元测试开始之前执行
    tearDown---对资源的释放，在所有的单元测试运行结束的时候执行。
     */
    @Before
    public void setUp()throws Exception{
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(hdfs_path),configuration,"root");//这里get方法有多个，根据要传入的参数选择对应的get方法创建fileSystem对象

    }

    @After
    public void tearDown() throws Exception{
        configuration = null;
        fileSystem = null;
        System.out.println("HDFSApp.tearDown...");

    }
}
