package pf.bluemoon.com.hadoop.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author: chaoyou
 * Email：1277618785@qq.com
 * CSDN：https://blog.csdn.net/qq_41910568
 * Date: 21:03 2022/8/15
 * Content：
 */
public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 取出行数据
        String line = value.toString();

        boolean pass = true;

        // etl清洗逻辑：筛选出每行字段长度都大于15
        String[] worlds = line.split(" ");
        if (worlds.length < 15){
            pass = false;
        }
        if (pass){
            context.write(value, NullWritable.get());
        }
    }
}
