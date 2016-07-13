package com.cstnet.cnnic.util;

/**
 * Created by biantao on 16/7/13.
 */
public class Assert {

    public static boolean isEmpty(String[] strings) {
        if (strings == null || strings.length == 1) return true;
        else return false;
    }

    public static boolean isEmpty(String string) {
        if (string == null) return true;
        else return false;
    }
}
