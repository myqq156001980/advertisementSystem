import com.sun.org.apache.xpath.internal.Arg;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Hunter on 14/11/7.
 */
public class MyRun {


    //输入输出路径
    private static String path="hdfs://localhost/";
//    private static String path="";
    private static String in1;
    private static String dir;
    private static String out1="/mac_phrase";
    private static String out2="/mac_words";
    private static String out3="/part_macvector";
    private static String out4="/active_macvector";
    private static String out5="/all_macvector";
    private static String out6="/rec_list";

    private static String keyword=path+"keyword/keyword.txt";
    private static String macvector=path+"macvector/macvector.txt";
    private static String ad=path+"ad/ad.txt";


    public static String getKeyword() {
        return keyword;
    }

    public static void setKeyword(String keyword) {
        MyRun.keyword = keyword;
    }

    public static String getMacvector() {
        return macvector;
    }

    public static void setMacvector(String macvector) {
        MyRun.macvector = macvector;
    }

    public static String getAd() {
        return ad;
    }

    public static void setAd(String ad) {
        MyRun.ad = ad;
    }

    //主函数
    public static void main(String[] Args) throws Exception {

        in1=Args[0];
        dir=Args[1];

        Configuration conf=new Configuration();

        if(Args.length!=2){
            System.err.println("Please use in and out");

            System.exit(2);
        }


        //MyCrawler
        Job jobCrawler=Job.getInstance(conf);

        jobCrawler.setJarByClass(MyCrawler.class);

        //设置map and reduce 处理类
        jobCrawler.setMapperClass(MyCrawler.Map.class);

        jobCrawler.setReducerClass(MyCrawler.Reduce.class);

        //设置map输出类型
        jobCrawler.setMapOutputKeyClass(Text.class);

        jobCrawler.setMapOutputValueClass(Text.class);

        //设置reduce输出类型

        jobCrawler.setOutputKeyClass(Text.class);

        jobCrawler.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobCrawler, new Path(path+in1));

        FileOutputFormat.setOutputPath(jobCrawler, new Path(path+dir+out1));

        jobCrawler.waitForCompletion(true);


        //MyIkAnalyzer
        Job jobAnalyzer=Job.getInstance(conf);

        jobAnalyzer.setJarByClass(MyIkAnalyzer.class);

        //设置map and reduce 处理类
        jobAnalyzer.setMapperClass(MyIkAnalyzer.Map.class);

        jobAnalyzer.setCombinerClass(MyIkAnalyzer.Combiner.class);

        jobAnalyzer.setReducerClass(MyIkAnalyzer.Reduce.class);

        //设置map输出类型
        jobAnalyzer.setMapOutputKeyClass(Text.class);

        jobAnalyzer.setMapOutputValueClass(IntWritable.class);
        jobAnalyzer.setInputFormatClass(KeyValueTextInputFormat.class);

        //设置reduce输出类型

        jobAnalyzer.setOutputKeyClass(Text.class);

        jobAnalyzer.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobAnalyzer, new Path(path+dir+out1));

        FileOutputFormat.setOutputPath(jobAnalyzer, new Path(path+dir+out2));


        jobAnalyzer.waitForCompletion(true);



        //OutputVector
        Job jobOutputVector=Job.getInstance(conf);

        jobOutputVector.setJarByClass(OutputVector.class);

        //设置map and reduce 处理类
        jobOutputVector.setMapperClass(OutputVector.Map.class);

        jobOutputVector.setReducerClass(OutputVector.Reduce.class);

        //设置map输出类型
        jobOutputVector.setMapOutputKeyClass(Text.class);

        jobOutputVector.setMapOutputValueClass(Text.class);

        jobOutputVector.setInputFormatClass(KeyValueTextInputFormat.class);

        //设置reduce输出类型
        jobOutputVector.setOutputKeyClass(Text.class);

        jobOutputVector.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(jobOutputVector, new Path(path+dir+out2));

        FileOutputFormat.setOutputPath(jobOutputVector, new Path(path+dir+out3));

        jobOutputVector.waitForCompletion(true);



        //ActiveMacVector
        Job jobActiveMacVector=Job.getInstance(conf);

        jobActiveMacVector.setJarByClass(ActiveMacVector.class);

        //设置map and reduce 处理类
        jobActiveMacVector.setMapperClass(ActiveMacVector.Map.class);

        jobActiveMacVector.setCombinerClass(ActiveMacVector.Combiner.class);

        jobActiveMacVector.setReducerClass(ActiveMacVector.Reduce.class);

        //设置map输出类型
        jobActiveMacVector.setMapOutputKeyClass(Text.class);

        jobActiveMacVector.setMapOutputValueClass(Text.class);

        jobActiveMacVector.setInputFormatClass(KeyValueTextInputFormat.class);

        //设置reduce输出类型
        jobActiveMacVector.setOutputKeyClass(Text.class);

        jobActiveMacVector.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(jobActiveMacVector, new Path(path+dir+out3));
        FileOutputFormat.setOutputPath(jobActiveMacVector, new Path(path + dir + out4));

        jobActiveMacVector.waitForCompletion(true);




        //GenerateAllMacVector
        Job jobGenerateAllMacVector=Job.getInstance(conf);

        jobGenerateAllMacVector.setJarByClass(GenerateAllMacVector.class);

        //设置map and reduce 处理类
        jobGenerateAllMacVector.setMapperClass(GenerateAllMacVector.Map.class);

        jobGenerateAllMacVector.setReducerClass(GenerateAllMacVector.Reduce.class);

        //设置map输出类型
        jobGenerateAllMacVector.setMapOutputKeyClass(Text.class);

        jobGenerateAllMacVector.setMapOutputValueClass(Text.class);

        jobGenerateAllMacVector.setInputFormatClass(KeyValueTextInputFormat.class);

        //设置reduce输出类型
        jobGenerateAllMacVector.setOutputKeyClass(Text.class);

        jobGenerateAllMacVector.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(jobGenerateAllMacVector, new Path(path+dir+out4));

        FileInputFormat.addInputPath(jobGenerateAllMacVector, new Path(path+"macvector"));


        FileOutputFormat.setOutputPath(jobGenerateAllMacVector, new Path(path+dir+out5));

        jobGenerateAllMacVector.waitForCompletion(true);











        //GenerateAdList
        Job jobAdList=Job.getInstance(conf);

        jobAdList.setJarByClass(GenerateAdList.class);

        //设置map and reduce 处理类
        jobAdList.setMapperClass(GenerateAdList.Map.class);

        jobAdList.setCombinerClass(GenerateAdList.Combiner.class);

        jobAdList.setReducerClass(GenerateAdList.Reduce.class);

        //设置map输出类型


        jobAdList.setMapOutputValueClass(Text.class);

        jobAdList.setInputFormatClass(KeyValueTextInputFormat.class);

        //设置reduce输出类型
        jobAdList.setOutputKeyClass(Text.class);

        jobAdList.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobAdList, new Path(path+dir+out5));

        FileOutputFormat.setOutputPath(jobAdList, new Path(path+dir+out6));

        System.exit(jobAdList.waitForCompletion(true)?0:1);

    }

}
