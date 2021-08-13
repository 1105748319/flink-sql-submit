package com.caeri.sqlsubmit;

import org.apache.hadoop.fs.Path;
import sun.applet.Main;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class Test {
    public static void main(String[] args) throws IOException {
        String classFilePath = Test.class.getResource("/hive").getPath();
        System.out.println(classFilePath+"==================");
        Path hiveSite = new Path("/hive", "hive-site.xml");
        if (!hiveSite.toUri().isAbsolute()) {

        }

    }

}
