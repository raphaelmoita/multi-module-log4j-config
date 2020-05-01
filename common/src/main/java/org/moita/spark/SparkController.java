package org.moita.spark;

import org.apache.log4j.Level;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.SparkSession.builder;

public final class SparkController {

    private static SparkSession session;

    static {
        session = builder().appName("logging-test")
                           .master("local[*]")
                           .getOrCreate();
    }

    private SparkController() {}

    public static <T> Dataset<T> createDs(List<T> list, Class<T> clazz) {
        return session.createDataset(list, Encoders.bean(clazz));
    }
}