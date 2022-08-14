package pf.bluemoon.com.hadoop.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.beans.BeanUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: chaoyou
 * Email：1277618785@qq.com
 * CSDN：https://blog.csdn.net/qq_41910568
 * Date: 18:50 2022/8/14
 * Content：
 */
public class JoinReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        List<TableBean> orderList = new ArrayList<>();
        TableBean pdBean = new TableBean();
        for (TableBean value : values) {
            if ("order".equals(value.getTabName())){
                TableBean orderBean = new TableBean();
                BeanUtils.copyProperties(value, orderBean);
                orderList.add(orderBean);
            } else {
                BeanUtils.copyProperties(value, pdBean);
            }
        }
        for (TableBean order : orderList) {
            order.setProName(pdBean.getProName());
            context.write(order, NullWritable.get());
        }
    }
}
