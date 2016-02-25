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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by Hunter on 14-10-26.
 */
public class GenerateAdList {

    public static HashMap<String,HashMap<String,String>> inputAdVector(String path){
        HashMap<String,HashMap<String,String>> adVector=new HashMap<String, HashMap<String, String>>();
        BufferedReader in=null;

        try{
            Configuration conf=new Configuration();
            FileSystem file=FileSystem.get(conf);
            FSDataInputStream fin=file.open(new Path(path));
            String line;
            in=new BufferedReader(new InputStreamReader(fin,"UTF-8"));
            while((line=in.readLine())!=null){
                HashMap<String,String> hashMap1=new HashMap<String, String>();
                String []strings=line.trim().split(" ");
                String adID=strings[0];
                for(int i=1;i<strings.length;i++){
                    String []s=strings[i].split(":");
                    hashMap1.put(s[0],s[1]);
                }

                if(!("".equals(adID))){
                    adVector.put(adID,hashMap1);
                }
            }
            return adVector;

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



    public static ArrayList<String>sort(HashMap<String,Double> hashMap){
        DecimalFormat df=new DecimalFormat("#.0000");
        ArrayList<String> list=new ArrayList<String>();
        int size=hashMap.size();
        double[] darr=new double[size];
        String[] sarr=new String[size];
        Set s=hashMap.keySet();

        Iterator it=s.iterator();
        int flag=0;
        int flag1=0;
        int cursor=0;
        double d;
        while(it.hasNext()){
            String temp=(String)it.next();
            d=hashMap.get(temp);

            if(flag==0){
                darr[0]=d;
                sarr[0]=temp;
                flag=1;
                cursor++;
                continue;
            }

            for(int i=cursor-1;i>=0;i--){
                if(d<=darr[i]){
                    for(int j=cursor;j>i;j--){
                       darr[j]=darr[j-1];
                       sarr[j]=sarr[j-1];
                    }
                    darr[i]=d;
                    sarr[i]=temp;
                    flag1=1;
                    break;

                }
            }
            if(flag1==1){
                flag1=0;
                cursor++;
                continue;
            }
            darr[cursor]=d;
            sarr[cursor]=temp;
            cursor++;
        }

        int top=5;
        if(darr.length<5){
            top=darr.length;
        }


        for(int i=0;i<top;i++){
            double d1=darr[i];
            String s1=sarr[i];
            String temp=s1+":"+df.format(d1);
            list.add(temp);
        }

        return list;
    }


    public static class Map extends Mapper<Text,Text,Text,Text>{
        private static String path=MyRun.getAd();
        private static HashMap<String,HashMap<String,String>> adVector=inputAdVector(path);

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            double total=0;
            String line=value.toString().trim();
            String [] kw=line.split(" ");

            for(String temp:kw){
                String [] s1=temp.split(":");
                double md=Double.parseDouble(s1[1]);
                total+=md;
            }

            for(String temp:kw){
                String [] s1=temp.split(":");

                Set s=adVector.keySet();
                Iterator it=s.iterator();
                while(it.hasNext()){
                    String adID=(String)it.next();
                    HashMap<String,String> hashMap;
                    hashMap=adVector.get(adID);
                    if(hashMap.containsKey(s1[0])){
                        Double d1;
                        Double d2;
                        d1=Double.parseDouble(hashMap.get(s1[0]));
                        d2=Double.parseDouble(s1[1]);


                        context.write(new Text(key+"@"+adID),new Text((d1.doubleValue()-d2.doubleValue()/total)+""));

                    }

                }

            }

        }
    }

    public static class Combiner extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String keyLine=key.toString().trim();
            String []keyLineSeparate=keyLine.split("@");
            double sum=0;
            for(Text value:values){
                String valueLine=value.toString().trim();
                double temp=Double.parseDouble(valueLine);

                sum+=temp*temp;
            }
            context.write(new Text(keyLineSeparate[0]),new Text(keyLineSeparate[1]+":"+(Math.sqrt(sum))));
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String,Double> hashMap=new HashMap<String, Double>();
            for(Text value:values){
                String valueLine=value.toString();
                String [] valueSeparate=valueLine.split(":");
                hashMap.put(valueSeparate[0],Double.parseDouble(valueSeparate[1]));

            }


            ArrayList<String> fList=sort(hashMap);

            String com="";
            for(int i=0;i<fList.size();i++){
                com+=fList.get(i);
                com+=" ";
            }

            context.write(key,new Text(com));


        }

    }


}
