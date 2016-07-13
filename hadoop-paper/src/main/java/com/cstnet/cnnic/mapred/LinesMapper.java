package com.cstnet.cnnic.mapred;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by biantao on 16/7/13.
 * 说明:
 * 任务会根据输入的目录产生inputsplit数组,每个数组对应一个map操作
 * map数量设置的太小,则并行度太低,若map的数量设置的太大,则调度的开销将会加大
 * LinesInputFormat的作用将是将输入的文本文件按行进行切分,每NumSplits(default:2)行输出到一个map
 * 通过运行时 -Dmapreduce.input.lineinputformat.linespermap来指定行数
 */
public class LinesMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    public LinesMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);
    }
}
