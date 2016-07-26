package com.cstnet.cnnic.mapred;

import com.cstnet.cnnic.VideoDistributed;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;

/**
 * Created by biantao on 16/7/13.
 * 说明:
 * 任务会根据输入的目录产生inputsplit数组,每个数组对应一个map操作
 * map数量设置的太小,则并行度太低,若map的数量设置的太大,则调度的开销将会加大
 * LinesInputFormat的作用将是将输入的文本文件按行进行切分,每NumSplits(default:2)行输出到一个map
 * 通过运行时 -Dmapreduce.input.lineinputformat.linespermap来指定行数
 */
public class LinesMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    public static final Log LOG = LogFactory.getLog(LinesMapper.class);

    private static final String localDir = "/tmp/biantao/code";
    private static String currentCodePath = "";

    public LinesMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        // 根据执行模式判断是可执行文件还是shell
        String mode = conf.get(VideoDistributed.EXE_MODE);
        if (mode.equals(VideoDistributed.DEFAULT_EXE_MODE)) {
            LOG.info("[current running mode is shell]");
            return;
        }
        // 如果是可执行文件模式,则需要将文件下拉倒本地执行
        String codePath = conf.get(VideoDistributed.SOURCE_PATH);
        File path = new File(localDir);
        if (!path.exists()) {
            LOG.info(String.format("[%s local %s not exist, create it first]", getDescription(context), localDir));
            if (path.mkdirs()) LOG.info(String.format("[directory %s created success]", localDir));
            else LOG.info(String.format("[directory %s created fail]", localDir));
        }
        LOG.info(String.format("[%s download %s from hdfs to %s]", getDescription(context), codePath, localDir));
        FileSystem fs = FileSystem.get(conf);
        Path dest = new Path(codePath);
        currentCodePath = localDir + "/" + System.currentTimeMillis() / 1000 + "_" + dest.getName();
        FSDataInputStream fsin = fs.open(dest);
        OutputStream output = new FileOutputStream(currentCodePath);
        IOUtils.copyBytes(fsin, output, 4096, true);
        LOG.info(String.format("[%s hdfs %s download to local %s]", getDescription(context), codePath, currentCodePath));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String mode = conf.get(VideoDistributed.EXE_MODE);
        if (mode.equals(VideoDistributed.DEFAULT_EXE_MODE)) {
            // 开始执行shell
            String cmds = conf.get(VideoDistributed.SOURCE_PATH);
            LOG.info(String.format("[executable shell is %s]", cmds));
            return;
        }
        // 执行可执行文件
        File file = new File(currentCodePath);
        LOG.info(String.format("[run.sh path is : %s]", currentCodePath));
        String[] cmds = {"sh", currentCodePath};
        try {
            Process ps = Runtime.getRuntime().exec(cmds);
            LOG.info(loadStream(ps.getInputStream()));
            LOG.error(loadStream(ps.getErrorStream()));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
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

    public String getDescription(Context context) {
        return "TaskAttemptID:" + context.getTaskAttemptID();
    }

    public String loadStream(InputStream in) throws IOException {
        int ptr = 0;
        in = new BufferedInputStream(in);
        StringBuffer buffer = new StringBuffer();
        while ((ptr = in.read()) != -1) {
            buffer.append((char) ptr);
        }
        return buffer.toString();
    }
}
