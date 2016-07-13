package com.cstnet.cnnic;

import com.cstnet.cnnic.util.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by biantao on 16/7/13.
 * package command:
 * mvn clean package -Dmaven.test.skip=true
 */
public class VideoDistributed extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(VideoDistributed.class);
    // default params
    private static final String NLINES = "mapreduce.input.lineinputformat.linespermap";
    private static final int DEFAULT_NLINES = 2;
    private static final String NREDUCER = "mapred.reduce.tasks";
    private static final int DEFAULT_NREDUCER = 1;
    private static final String SORT = "cnic.sort";


    public static void main(String[] args) throws Exception {
        LOG.info(String.format("%s start...", getDescription()));
        int res = ToolRunner.run(new Configuration(), new VideoDistributed(), args);
        LOG.info(String.format("%s finished, exit with stauts %s", getDescription(), res));
    }

    protected static String getDescription() {
        return "VideoDistributed Task";
    }

    private static void printUsage() {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("Usage: VideoDistributed [-options] <command> [args...]\n" +
                "\n" +
                "general options:\n" +
                "-Dmapreduce.input.lineinputformat.linespermap=2 (default:2)\n" +
                "-Dmapred.reduce.tasks=1 (default:1)\n" +
                "-Dcnic.sort=0\t\t\t\t(default:1) sort the records of file by their size, (1 sort 0 not)" +
                "-D etc..." +
                "\n\n" +
                "commands:\n" +
                "batch\t\t\t\tprocess some videos at the same time\n" +
                "single\t\t\t\tprocess a single video at the same time" +
                "\n\n" +
                "args:\n" +
                "inputpath\t\t\tinput files\n" +
                "outputpath\t\t\toutput file location" +
                "\n\n" +
                "example:" +
                "hadoop jar hadoop-paper-1.0-SNAPSHOT.jar com.cstnet.cnnic.VideoDistributed -Dmapreduce.input.lineinputformat.linespermap=2 " +
                "-Dmapred.reduce.tasks=1 " +
                "batch " +
                "hdfs:* " +
                "hdfs:*\n");
        System.out.println(stringBuffer.toString());
    }

    /**
     * 根据输入文件(文件每行代表一个文件的hdfs地址)
     * 根据大小排序纪录
     * @param file
     * @param split 计划的map数量
     * Point1:
     * 尽量每个map中size加起来的和相差最小
     */
    private void sortRecordsBySize(String file, int split) {
        LOG.info("sort records by their size");

    }

    public int run(String[] strings) throws Exception {
        if (Assert.isEmpty(strings) || strings.length != 3) {
            printUsage();
            System.exit(0);
        }

        Configuration conf = getConf();
        if (Assert.isEmpty(conf.get(NLINES))) {
            conf.setInt(NLINES, DEFAULT_NLINES);
            LOG.info(String.format("%s not set, use default %s", NLINES, DEFAULT_NLINES));
        }
        if (Assert.isEmpty(conf.get(NREDUCER))) {
            conf.setInt(NREDUCER, DEFAULT_NREDUCER);
            LOG.info(String.format("%s not set, use default %s", NREDUCER, DEFAULT_NREDUCER));
        }

        LOG.info("parse command and options");
        String command = strings[0];
        String input = strings[1];
        String output = strings[2];
        if (command.equals("batch")) {
            LOG.info(String.format("command is batch; input file is %s; output is %s", input, output));
            if (conf.getInt(SORT, 1) == 1) {
                sortRecordsBySize(input, conf.getInt(NLINES, DEFAULT_NLINES));
            }
            System.exit(0);
        } else {
            LOG.info(String.format("command %s is wrong or not implement this command"));
            System.exit(0);
        }
        return 0;
    }
}
