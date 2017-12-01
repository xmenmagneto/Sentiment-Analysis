import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SentimentAnalysis {

    //inner class
    public static class SentimentSplit extends Mapper<Object, Text, Text, IntWritable> {

        public Map<String, String> emotionLibrary = new HashMap<String, String>();

        @Override
        public void setup(Context context) throws IOException { //初始化后，不再改变
            //create emotionLibrary
            //通过getConfiguration，动态的找到emotionCategory.txt的路径
            Configuration configuration = context.getConfiguration();
            String path = configuration.get("dictionary");  //不可以写死，让user自己define，让user从命令行传入

            BufferedReader br = new BufferedReader(new FileReader(path));
            String line = br.readLine();

            while (line != null) {
                //line = "aquatic   neutral"
                String[] word_feeling = line.split("\t");
                emotionLibrary.put(word_feeling[0], word_feeling[1]);
                line = br.readLine(); //read next line
            }
            br.close();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //key 是offset:第几篇中的第几行 -> 位置
            //value 是文章中的每一行 = line

            //read file
            //split into single words
            //look up sentimentLibrary -> sentiment
            //write out key value
            String[] words = value.toString().split("\\s+"); //split by space
            for (String word : words) {
                if (emotionLibrary.containsKey(word.toLowerCase())) { //具有情感的单词
                    String sentiment = emotionLibrary.get(word.toLowerCase());
                    context.write(new Text(sentiment), new IntWritable(1));
                }
            }
        }
    }

    public static class SentimentCollection extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        //override reduce方法
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            //收集mapper所有的数据，然后做merge
            //key = positive
            //value = <1,1,1,1,1,1,1,1,1...>
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }

    }

    public static void main(String[] args) throws Exception {

    }
}
