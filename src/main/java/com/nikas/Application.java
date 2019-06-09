package com.nikas;

import static org.apache.spark.sql.functions.*;

import com.nikas.dto.Log;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Application {
    //log entry
    private static final String LOG_ENTRY_PATTERN =
        "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
    private static final Pattern LOG_PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);
    //hdfs
    private static final String TASK_1 = "1";
    private static final String TASK_2 = "2";
    private static final String TASK_3 = "3";
    private static final String HDFS = "hdfs://master:9000/";
    //parsing of dates
    private static final String OUTPUT_DATE_PATTERN = "dd/MMM/yyyy";
    private static final DateTimeFormatter LOG_FORMATTER = DateTimeFormatter
        .ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter
        .ofPattern(OUTPUT_DATE_PATTERN, Locale.US);
    //log entry columns
    private static final String COLUMN_CODE = "code";
    private static final String COLUMN_REQUEST = "request";
    private static final String COLUMN_METHOD = "method";
    private static final String COLUMN_DATE = "date";
    //util
    private static final String COLUMN_COUNT = "count";

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("parser").getOrCreate();

        JavaRDD<Log> rdd = sparkSession.read().textFile(HDFS + "/logs/log")
            .javaRDD()
            .map(e -> parseLogLine(e))
            .filter(Objects::nonNull);

        Dataset<Row> dataSet = sparkSession.createDataFrame(rdd, Log.class);

        parseTaskOne(dataSet);
        parseTaskTwo(dataSet);
        parseTaskThree(dataSet);
    }

    private static Log parseLogLine(String line) {
        try {
            Matcher matcher = LOG_PATTERN.matcher(line);
            LocalDate logDateTime = LocalDate.parse(matcher.group(4), LOG_FORMATTER);
            return new Log(matcher.group(5), matcher.group(6), matcher.group(8), logDateTime.format(DATE_FORMATTER));
        } catch (Exception e) {
            return null;
        }
    }

    private static void parseTaskOne(Dataset<Row> dataSet) {
        dataSet.filter(col(COLUMN_CODE).between(500, 599))
            .groupBy(COLUMN_REQUEST)
            .count()
            .select(COLUMN_REQUEST, COLUMN_COUNT)
            .coalesce(1)
            .toJavaRDD()
            .saveAsTextFile(HDFS + TASK_1);
    }

    private static void parseTaskTwo(Dataset<Row> dataSet) {
        dataSet.groupBy(COLUMN_METHOD, COLUMN_CODE, COLUMN_DATE)
            .count()
            .filter(col(COLUMN_COUNT).geq(10))
            .select(COLUMN_DATE, COLUMN_METHOD, COLUMN_CODE, COLUMN_COUNT)
            .sort(COLUMN_DATE)
            .coalesce(1)
            .toJavaRDD()
            .saveAsTextFile(HDFS + TASK_2);
    }

    private static void parseTaskThree(Dataset<Row> dataSet) {
        dataSet.filter(col(COLUMN_CODE).between(300, 600))
            .groupBy(window(to_date(col(COLUMN_DATE), OUTPUT_DATE_PATTERN), "1 week", "1 day"))
            .count()
            .select(date_format(col("window.start"), OUTPUT_DATE_PATTERN),
                date_format(col("window.end"), OUTPUT_DATE_PATTERN),
                col(COLUMN_COUNT))
            .sort("window.start")
            .coalesce(1)
            .toJavaRDD()
            .saveAsTextFile(HDFS + TASK_3);
    }

}
