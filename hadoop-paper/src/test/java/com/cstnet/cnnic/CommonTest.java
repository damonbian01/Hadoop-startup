package com.cstnet.cnnic;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Created by biantao on 16/7/13.
 */
public class CommonTest {

    @Test
    public void testPath() {
        Path path = new Path("/tmp/biatao/file");
        System.out.println(path.getName());
        System.out.println(path.getParent());
        Path spath = new Path(path, "sub_file");
        System.out.println(spath.getName());
        System.out.println(spath.getParent());

    }
}
