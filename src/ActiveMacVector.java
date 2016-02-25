import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by Hunter on 14/11/7.
 */
public class ActiveMacVector {




    //读入macid,vector列表
    public static HashMap<String,HashMap<String,String>> inputMacVector(String path){
        HashMap<String,HashMap<String,String>> macVector=new HashMap<String, HashMap<String, String>>();


        BufferedReader in=null;

        try{
            Configuration conf=new Configuration();
            FileSystem file=FileSystem.get(conf);
            FSDataInputStream fin=file.open(new Path(path));
            String line;
            in=new BufferedReader(new InputStreamReader(fin,"UTF-8"));
            while((line=in.readLine())!=null){
                HashMap<String,String> hashMap1=new HashMap<String, String>();
                String []strings1=line.trim().split("\t");
                String macID2=strings1[0];
                String [] vector=strings1[1].trim().split(" ");
                for(int i=0;i<vector.length;i++){
                    String []s=vector[i].split(":");
                    hashMap1.put(s[0],s[1]);
                }

                if(!("".equals(macID2))){
                    macVector.put(macID2,hashMap1);
                }
            }
            return macVector;

        }catch(IOException e){
            e.printStackTrace();
            return null;
        }
    }

    //划分向量字符串
    public static HashMap<String,String> splitVString(String s){
        HashMap<String,String> hashMap2=new HashMap<String, String>();
        String[] vw=s.split(" ");
        for(String value:vw){
            if(!("".equals(value))){
                String []temp=value.split(":");
                hashMap2.put(temp[0],temp[1]);
            }
        }
        return hashMap2;
    }


    public static class Map extends Mapper<Text,Text,Text,Text>{

        private static String path=MyRun.getMacvector();
        private static HashMap<String,HashMap<String,String>> macVector=inputMacVector(path);


        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            ArrayList<String> active_keyword=new ArrayList<String>();
            String line=value.toString().trim();
            HashMap<String,String> hashMap1=splitVString(line);
            Set setKW1=hashMap1.keySet();
            Iterator iteratorKW=setKW1.iterator();
            while(iteratorKW.hasNext()){
                String keyWord1=iteratorKW.next().toString();
                active_keyword.add(keyWord1);
                String weight=hashMap1.get(keyWord1);
                context.write(new Text(key.toString()+"@"+keyWord1),new Text(weight));

            }

            try{
                Set setFormer=macVector.keySet();
                if(setFormer.contains(key.toString())){
                    HashMap<String,String> hashMap=macVector.get(key.toString());
                    Set setKW2=hashMap.keySet();
                    Iterator iteratorKW2=setKW2.iterator();
                    while(iteratorKW2.hasNext()){
                        String keyWord2=iteratorKW2.next().toString();
                        if(active_keyword.contains(keyWord2)){
                            String weight=hashMap.get(keyWord2);
                            context.write(new Text(key.toString()+"@"+keyWord2),new Text(weight));
                        }else {
                            String temp=hashMap.get(keyWord2);
                            int weight=(Integer.parseInt(temp)/2);

                            context.write(new Text(key.toString()+"@"+keyWord2),new Text(weight+""));
                        }

                    }

                }

            }catch (NullPointerException e){
                e.printStackTrace();
            }
        }
    }


    //合并map存入的结果
    public static class Combiner extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String line=key.toString().trim();
            String [] mk=line.split("@");
            int sum=0;
            for(Text value:values){
                int i=Integer.parseInt(value.toString().trim());
                sum+=i;
            }
            context.write(new Text(mk[0]),new Text(mk[1]+":"+sum));
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String vector="";
            for(Text value:values){
                vector+=value.toString();
                vector+=" ";
            }

            vector+=1;

            context.write(key,new Text(vector));

        }
    }

}
