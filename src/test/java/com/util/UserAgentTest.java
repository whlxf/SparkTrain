package com.util;

/*单机版的统计数量*/

import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.kumkee.userAgent.UserAgentParser;
import com.kumkee.userAgent.UserAgent;
/*
* UserAgent测试类
* */
public class UserAgentTest {

    public static void main(String[] args) {

        //source：一条日志信息记录

        String source = "Accept: */*\\n\" +\n" +
                " \"Accept-Encoding: gzip, deflate, br\\n\" +\n" +
                " \"Accept-Language: zh-CN,zh;q=0.9\\n\" +\n" +
                " \"Connection: keep-alive\\n\" +\n" +
                " \"Content-Length: 11585\\n\" +\n" +
                " \"Content-Type: application/json\\n\" +\n" +
                " \"Host: api.github.com\\n\" +\n" +
                " \"Origin: https://github.com\\n\" +\n" +
                " \"Referer: https://github.com/LeeKemp/UserAgentParser\\n\" +\n" +
                "\"User-Agent: Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36";

        //使用UserAgentParser解析类（导入包）
        UserAgentParser userAgentParser  = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);

        //之后就是agent.方法来获取被解析的值
        String browser = agent.getBrowser();

    }

    /*2
    * 单元测试:UserAgent工具类的使用
    * */

    public void testUserAgent(){

        String source = "";
        UserAgentParser userAgentParser  = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);

        String browser = agent.getBrowser();
        //.方法来获取一些信息
        System.out.println("Browser: " + browser);
    }


    public void testReadFile() throws Exception{
        String filePath = "C:\\Users\\DELL\\Desktop\\datatest.txt";
        //BufferedReader()要加上一个字节流和字符流的转换,拿到reader
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filePath))));
        //拿到reader之后，一行一行进行处理数据
        String line = "";

        UserAgentParser userAgentParser  = new UserAgentParser();

        //最后，统计不同浏览器访问的次数
        Map<String, Integer> browserMap = new HashMap<String, Integer>(); //<浏览器名称,数量>

        //考虑是否会有被丢失的日志信息，加一个记录数
        int i = 0;
        while (line != null){
            line = reader.readLine();//一次读入一行数据
            i++;
            //读进来之后，要考虑是否为空，为空再处理，不为空就算了，养成好习惯
            if (StringUtils.isNotBlank(line)){
                //解析数据，要去看数据有没有什么规律可循，比如 分割符。找到比较好的解析要点之后，定义一个方法实现解析，在这里调用。
                String  source = line.substring(getCharacterPosition(line,":",9)); //+1表示从:后面开始取，到最后
                UserAgent agent = userAgentParser.parse(source);
                //agent.方法 打印结果，查看是否有错误。
                String browser = agent.getBrowser();

                //处理数据开始
                if (browserMap.get(browser) != null){
                    browserMap.put(browser,browserMap.get(browser) + 1); //不为空说明不是第一次取
                }else {
                    //第一次取
                    browserMap.put(browser,1);
                }
            }
        }
        System.out.println(" UserAgentTest.testReadFile records : " + i);
        //增强for循环 ，迭代map
        for (Map.Entry<String, Integer> entry : browserMap.entrySet()){
            System.out.println(entry.getKey() + " : " + entry.getValue()); //这就可以打印出不同浏览器对应的访问量了
        }
    }

    //自定义一个方法，专门用来解析行 数据的

    /*获取指定字符串中指定标识的字符串出现的索引位置
    * @param
    * @param
    * @param
    * @return
    * */
    private int getCharacterPosition(String value,String operator,int index){
        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        int mIdx = 0;
        while(slashMatcher.find()){
            mIdx++;
            if (mIdx == index){
                break;
            }
        }
        return slashMatcher.start();
    }

    //写完自定义方法之后，好习惯测试一下

    /*
    *测试自定义方法：getCharacterPosition
    * */

    public void testGetCharacterPosition(){
        String value = "Accept: */*\n" +
                "Accept-Encoding: gzip, deflate, br\n" +
                "Accept-Language: zh-CN,zh;q=0.9\n" +
                "Connection: keep-alive\n" +
                "Content-Length: 11585\n" +
                "Content-Type: application/json\n" +
                "Host: api.github.com\n" +
                "Origin: https://github.com\n" +
                "Referer: https://github.com/LeeKemp/UserAgentParser\n" +
                "User-Agent: Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36";
        int index = getCharacterPosition(value,":",9);
        System.out.println(index);
    }


}
