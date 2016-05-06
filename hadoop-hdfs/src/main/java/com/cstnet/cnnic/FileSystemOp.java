package com.cstnet.cnnic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Random;

/**
 * Created by Tao Bian 2016/5/6.
 * use FileSystem API to operate HDFS
 */
public class FileSystemOp {

    public static void printUsage() {
        System.out.println("\t\t>>>>>>>>>>>>>>>Warn:incorrect params<<<<<<<<<<<<<<<");
        System.out.println("\t\t<usage>");
        System.out.println("\t\t\thadoop jar hadoop-hdfs-1.0-SNAPSHOT.jar com.cstnet.cnnic.FileSystemOp [0|1|2]");
        System.out.println("\t\t\t0:opSample();");
        System.out.println("\t\t\t1:readSample();");
        System.out.println("\t\t\t2:writeSample();");
        System.out.println("\t\t</usage>");
    }

    public static void opSample() throws IOException {
        /*read core-site.xml*/
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        /*Path '/user/<current_user>/user/test' will be created*/
        Path dir = new Path("user/test");
        Path file = new Path(dir,"a.txt");
        fs.mkdirs(dir);
        fs.copyFromLocalFile(new Path("a.txt"), file);
        FileStatus statue = fs.getFileStatus(file);
        /*get the absolute path*/
        System.out.println(statue.getPath());
        /*get the relative path*/
        System.out.println(statue.getPath().toUri().getPath());
        /*get current block size*/
        System.out.println(statue.getBlockSize());
        /*get user group*/
        System.out.println(statue.getGroup());
        /*get file owner*/
        System.out.println(statue.getOwner());
        fs.delete(file,true);
        fs.delete(dir,true);
        /*confirm whether the file is deleted*/
        System.out.println(fs.isFile(file));
        System.out.println(fs.isDirectory(dir));
    }

    public static void readSample() throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path("/user/biantao/hadoop-startup/hdfs/read_sample.txt");
        FileSystem fs = FileSystem.get(conf);
        /*verify whether the file is exist*/
        if (!fs.isFile(path)) {
            System.out.println("/user/biantao/hadoop-startup/hdfs/read_sample.txt is not exist");
            System.exit(1);
        }
        /*create input stream*/
        FSDataInputStream fsin = fs.open(path);
        /*create cache array*/
        byte[] buffer = new byte[128];
        int length = 0;
        while ((length = fsin.read(buffer,0,128)) != -1) {
            System.out.print(new String(buffer,0,length));
        }
    }

    public static void writeSample() throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path("/user/biantao/hadoop-startup/hdfs/write_sample.txt");
        FileSystem fs = FileSystem.get(conf);
        /*create input buffer*/
        StringBuffer sb = new StringBuffer();
        Random rand = new Random();
        for (int i = 0; i < 9999999; i++) {
            sb.append((char)rand.nextInt(100));
        }
        byte[] inputBuffer = sb.toString().getBytes();
        /*create write stream, when 64k has be written, it will call progress*/
        FSDataOutputStream fsout = fs.create(path, new Progressable() {
            int index = 0;
            public void progress() {
                int size = 64*(++index);
                 System.out.println(size+"K has been written");
            }
        });
        fsout.write(inputBuffer);
        /*close write stream*/
        IOUtils.closeStream(fsout);
    }

    public static void main(String [] args) {
        if (args == null || args.length < 1) {
            printUsage();
            System.exit(1);
        }

        int type = 0;
        try {
            type = Integer.valueOf(args[0]);
            switch (type) {
                case 0:
                    opSample();break;
                case 1:
                    readSample();break;
                case 2:
                    writeSample();break;
                default:
                    printUsage();break;
            }
        } catch (NumberFormatException e) {
            printUsage();
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
