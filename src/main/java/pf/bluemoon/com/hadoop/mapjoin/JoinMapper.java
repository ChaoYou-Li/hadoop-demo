package pf.bluemoon.com.hadoop.mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import pf.bluemoon.com.hadoop.reducejoin.TableBean;
import pf.bluemoon.com.hadoop.utils.NumberUtil;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: chaoyou
 * Email：1277618785@qq.com
 * CSDN：https://blog.csdn.net/qq_41910568
 * Date: 18:39 2022/8/14
 * Content：
 */
public class JoinMapper extends Mapper<LongWritable, Text, TableBean, NullWritable> {

    private TableBean outKey = new TableBean();

    private Map<String, String> cache = new HashMap<>();

    /**
     * 获取缓存文件数据，并把数据读出来存放到一个map中
     */
    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream stream = fs.open(new Path(cacheFiles[0]));
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
        String line;
        while (null != (line = reader.readLine())){
            if (line.length() < 1){
                continue;
            }
            String[] worlds = line.split("\t");
            cache.put(worlds[0], worlds[worlds.length - 1]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (null == line || line.length() < 1){
            return;
        }
        String[] worlds = line.split("\t");
        /**
         * 订单表处理逻辑
         */
        long amount = NumberUtil.isNumber(worlds[worlds.length - 1]) ? Long.parseLong(worlds[worlds.length - 1]) : 0;
        outKey.setId(worlds[0]);
        outKey.setAmount(amount);
        outKey.setPid(worlds[worlds.length - 2]);
        outKey.setProName(cache.get(outKey.getPid()));
        outKey.setTabName("order");

        // 输出
        context.write(outKey, NullWritable.get());
    }
}
