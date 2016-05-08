package com.cstnet.cnnic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Tao Bian on 2016/5/6.
 * from hadoop-2.6.2-src and add multi threads
 */
public class WordCount extends Configured implements Tool {

    /**
     * use source-code method to run
     * @param otherArgs
     * @return
     */
    private int confSys(Configuration conf, String[] otherArgs) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job(conf, "word count");
        job.setNumReduceTasks(1);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * use my method to run
     * @param otherArgs
     * @return
     */
    private int confSelf(Configuration conf, String[] otherArgs) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job(conf, "word count");
        job.setNumReduceTasks(1);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MultiTokenizerMapper.class);
        job.setCombinerClass(MultiIntSumReducer.class);
        job.setReducerClass(MultiIntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public int run(String[] otherArgs) throws Exception {
        int res = 0;
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount [-D mul-thread=true|false(default:false)] <in> [<in>...] <out>");
            System.exit(2);
        }

        Configuration conf = getConf();
        /*here must be equals, can't be == or it will be always false*/
        int threads = (conf.get("mul-thread") != null && conf.get("mul-thread").equals("true")) ? 1 : 0;
        switch (threads) {
            case 0:
                System.out.println(">>>>>>>>>>>>single thread to run wordcount<<<<<<<<<<<<<<");
                res = confSys(conf,otherArgs);
                ;break;
            case 1:
                System.out.println(">>>>>>>>>>>>multi thread to run wordcount<<<<<<<<<<<<<<");
                res = confSelf(conf, otherArgs);
                ;break;
            default:
                System.out.println(">>>>>>>>>>>>default single thread to run wordcount<<<<<<<<<<<<<<");
                res = confSys(conf, otherArgs);
                break;
        }
        return res;
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static  IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word,one);
            }
        }

    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }

    }

    /**
     * below is the multi threads map-reduce add by Tao Bian
     */
    public static class MultiTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static  IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word,one);
            }
        }
    }

    public static class MultiIntSumReducer extends  Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main (String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }
}
