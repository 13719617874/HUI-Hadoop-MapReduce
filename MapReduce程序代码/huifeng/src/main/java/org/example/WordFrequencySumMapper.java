package org.example;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordFrequencySumMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();
    private IntWritable frequency = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将输入的每一行数据分割成单词和频率
        String[] parts = value.toString().trim().split("\t");
        if (parts.length == 2) {
            String wordStr = parts[0];
            int count = Integer.parseInt(parts[1]);

            // 发送到Reducer的键值对是 <单词, 频率>
            word.set(wordStr);
            frequency.set(count);
            context.write(word, frequency);
        }
    }
}
