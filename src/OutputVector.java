
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.*;

/**
* Created by Hunter on 14-10-13.
*/
public class OutputVector {



    //读入分词文件
    public static class Map extends Mapper<Text,Text,Text,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            context.write(key,value);
        }
    }

    //统计用户访问网页的关键词进行输出
    public static class Reduce extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String temp="";
            for(Text value:values){
                temp+=value.toString()+" ";
            }
            context.write(key,new Text(temp));
        }


    }

}
