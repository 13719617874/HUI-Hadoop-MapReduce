package org.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordFrequencySumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // 对同一单词的频率进行累加
        for (IntWritable val : values) {
            sum += val.get();
        }

        // 输出最终的 <单词, 总频率数> 键值对
        result.set(sum);
        context.write(key, result);
    }
}
