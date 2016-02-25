
import net.sf.json.JSONArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;

/**
 * Created by Hunter on 14/12/2.
 */
public class MyCrawler {
    public static class Map extends Mapper<Object,Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*

            JSONArray jsonArr = JSONArray.fromObject(value);
            int length = jsonArr.size();
            for(int i=0;i<length;i++){
                String macID = jsonArr.getJSONObject(i).getString("macID");
                String url = jsonArr.getJSONObject(i).getString("url");
                try {
                    Document doc = Jsoup.connect(url.trim())
                            .data("query", "Java")
                            .userAgent("Mozilla")
                            .cookie("auth", "token")
                            .timeout(5000)
                            .get();
                    String title = doc.title();
                    Elements eles = doc.select("meta[name=keywords]");
                    for (Element e : eles) {
                        title += (" " + e.attr("content"));
                    }
                    context.write(new Text(macID),new Text(title));
                }catch (Exception e){
                    e.printStackTrace();

                }
            }
            */
            String line = value.toString().trim();

            String[] lines = line.split(" ");

//            context.write(new Text(), new Text(lines.length+""));

//            for(String str : lines){
//                context.write(new Text(), new Text(str));
//            }
            if(lines.length == 27){
                String macID = lines[9].substring(1,lines[9].length()-1);
                int num = lines.length-2;
                String url = lines[num].substring(1,lines[num].length()-1);
                try {
                    Document doc = Jsoup.connect(url.trim())
                            .data("query", "Java")
                            .userAgent("Mozilla")
                            .cookie("auth", "token")
                            .timeout(5000)
                            .get();
                    String title = doc.title();
                    Elements eles = doc.select("meta[name=keywords]");
                    for (Element e : eles) {
                        title += (" " + e.attr("content"));
                    }
                    context.write(new Text(macID),new Text(title));
                }catch (Exception e){
                    e.printStackTrace();

                }
            }





        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String temp="";
            for(Text value:values){
                temp+=value.toString()+" ";
            }
            context.write(key,new Text(temp));
//            qfor(Text value : values){
//                context.write(new Text(), value);
//            }
        }
    }

}
