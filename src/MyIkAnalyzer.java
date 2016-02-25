import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.wltea.analyzer.IKSegmentation;
import org.wltea.analyzer.Lexeme;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.util.ArrayList;


/**
 * Created by Hunter on 14-10-11.
 */
public class MyIkAnalyzer {


    //读入keyword到Arraylist
    public static ArrayList<ArrayList<String>> inputArrayList(String path){
        ArrayList<ArrayList<String>> wordsList=new ArrayList<ArrayList<String>>();
        Configuration conf=new Configuration();
        BufferedReader in=null;
        try{
            FileSystem fs=FileSystem.get(URI.create(path),conf);
            FSDataInputStream fin=fs.open(new Path(path));
            String line;
            in=new BufferedReader(new InputStreamReader(fin,"UTF-8"));
            while ((line=in.readLine())!=null){

                //保存关键词
                String key=null;

                //关键词所包含的词语
                ArrayList<String> list=new ArrayList<String>();
                String [] lineSeparate=line.split("\t");
                String []localWordList=lineSeparate[1].split(" ");

                for(String value:localWordList){
                    list.add(valuema);
                }
                wordsList.add(list);


            }
            return wordsList;
        }catch(IOException e){

            e.printStackTrace();
            return null;
        }finally {
            if(in!=null){
                try{
                    in.close();
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }

    }

    //读入需要分词的文件
    public static class Map extends Mapper<Text,Text,Text,IntWritable>{
        //记录mac地址

        //Map函数
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
           String line=value.toString().trim();
           String[] phrase=line.split(" ");
            ArrayList<String> words;

            for(int i=0;i<phrase.length;i++){
                words=separateWord(phrase[i]);
                //写入context
                for(int j=0;j<words.size();j++){
                    context.write(new Text(key.toString()+"@"+words.get(j)),new IntWritable(1));
                }

            }


        }
    }

    //写入文件
    public static class Combiner extends Reducer<Text,IntWritable,Text,IntWritable>{
        private static String path=MyRun.getKeyword();
        private static ArrayList<ArrayList<String>> wordsList=inputArrayList(path);
        //reduce函数
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] keySeparate=key.toString().split("@");
            String keyword=keySeparate[1];
            for(int i=0;i<wordsList.size();i++){
                int sum=0;
                ArrayList<String> temp=wordsList.get(i);
                if(temp.contains(keyword)){
                    for(IntWritable value:values){
                        sum+=value.get();
                    }
                    context.write(new Text(keySeparate[0]+"@"+temp.get(0)),new IntWritable(sum));
                    break;
                }
            }



        }
    }



    //写入文件
    public static class Reduce extends Reducer<Text,IntWritable,Text,Text>{
        private static String path=MyRun.getKeyword();
        private static ArrayList<ArrayList<String>> wordsList=inputArrayList(path);
        //reduce函数
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] keySeparate=key.toString().split("@");
            String keyword=keySeparate[1];
            for(int i=0;i<wordsList.size();i++){
                int sum=0;
                ArrayList<String> temp=wordsList.get(i);
                if(temp.contains(keyword)){
                    for(IntWritable value:values){
                        sum+=value.get();
                    }
                    context.write(new Text(keySeparate[0]),new Text(temp.get(0)+":"+sum));
                    break;
                }
            }


//            for (int i = 0; i <wordsList.size() ; i++) {
//                context.write(new Text(wordsList.get(i).get(0)), new Text());
//                ArrayList<String> temp=wordsList.get(i);
//                if(temp.contains("运动服")){
//                    context.write(new Text("存在"), new Text());
//                }
//            }


//            context.write(key,new Text());

        }
    }

    //分词函数
    public static ArrayList<String> separateWord(String line){
        IKSegmentation seg;
        StringReader reader=new StringReader(line);
        seg=new IKSegmentation(reader);
        ArrayList<String> words=new ArrayList<String>();
        Lexeme lex;

        try{
            while((lex=seg.next())!=null){
                String temp=lex.getLexemeText();
                words.add(temp);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return words;
    }



}
