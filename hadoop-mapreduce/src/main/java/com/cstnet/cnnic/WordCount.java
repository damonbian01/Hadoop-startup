package com.cstnet.cnnic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by Tao Bian on 2016/5/6.
 * from hadoop-2.6.2-src and add multi threads
 */
public class WordCount {

    public static void main (String[] args) throws IOException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount [-D mul-thread=true|false(default:false)] <in> [<in>...] <out>");
            System.exit(2);
        }

        /*here must be equals, can't be == or it will be always false*/
        boolean threads = (conf.get("mul-thread") != null && conf.get("mul-thread").equals("true")) ? true : false;
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>test GenericOptionsParser<<<<<<<<<<<<<<<<<<<<<");
        System.out.println(threads);
        System.exit(0);
    }
}
