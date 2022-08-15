package pf.bluemoon.com.hadoop.zip;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

/**
 * Author: chaoyou
 * Email：1277618785@qq.com
 * CSDN：https://blog.csdn.net/qq_41910568
 * Date: 21:09 2022/8/15
 * Content：压缩实战
 */
public class ETLDriver {
    public static void drive(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 初始化配置参数
        Configuration config = new Configuration();
        // 开启map端输出压缩
        config.setBoolean("mapreduce.map.output.compress", true);
        // 设置map端输出压缩方式
        config.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);


        Job job = Job.getInstance(config);

        job.setJarByClass(ETLDriver.class);

        job.setMapperClass(ETLMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 1 : 0);
    }
}
