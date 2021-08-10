package com.caeri.sqlsubmit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        try {
            List<String> sql = Files.readAllLines(Paths.get("D:\\zhongqi-project\\flink-sql-submit\\src\\main\\resources\\q1.sql"));
            System.out.printf("");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
