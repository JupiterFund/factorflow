package com.nodeunify.jupiter.spark.io;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Parser;
import com.nodeunify.jupiter.datastream.v1.FutureData;
import com.nodeunify.jupiter.datastream.v1.IndexData;
import com.nodeunify.jupiter.datastream.v1.OrderQueue;
import com.nodeunify.jupiter.datastream.v1.StockData;
import com.nodeunify.jupiter.datastream.v1.Transaction;

import org.apache.commons.math3.geometry.partitioning.BSPTreeVisitor.Order;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import scala.collection.JavaConversions;

@SuppressWarnings("unchecked")
public class KafkaIO {
    private final SparkSession spark;
    private LinkedHashMap<String, Object> sparkProps;
    private LinkedHashMap<String, String> sparkConfProps;
    private LinkedHashMap<String, Object> kafkaProps;

    public static KafkaIO create() {
        Map<String, Object> props = new HashMap<String, Object>();
        // TODO: set default properties
        KafkaIO kafkaIO = new KafkaIO(props);
        return kafkaIO;
    }

    public static KafkaIO create(Map<String, Object> props) {
        KafkaIO kafkaIO = new KafkaIO(props);
        return kafkaIO;
    }

    public SparkSession getSession() {
        return spark;
    }

    private KafkaIO(Map<String, Object> props) {
        kafkaProps = (LinkedHashMap<String, Object>) props.get("kafka");
        sparkProps = (LinkedHashMap<String, Object>) props.get("spark");
        sparkConfProps = (LinkedHashMap<String, String>) sparkProps.get("conf");

        SparkConf conf = new SparkConf().setAll(JavaConversions.mapAsScalaMap(sparkConfProps));
        // Register all supported data types of jupiterapis
        List<Class<?>> classes = Arrays.<Class<?>>asList(
            StockData.class,
            FutureData.class,
            IndexData.class,
            Order.class,
            OrderQueue.class,
            Transaction.class
        );
        conf.registerKryoClasses((Class<?>[]) classes.toArray());
        // @formatter:off
        spark = SparkSession
            .builder()
            .appName((String) sparkProps.get("appname"))
            .master((String) sparkProps.get("master"))
            .config(conf)
            .getOrCreate();
        // @formatter:on
        spark.sparkContext().setLogLevel("ERROR");
    }

    public <T> Dataset<T> readData(Class<T> clazz) throws IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, InstantiationException {
        return readData((String) kafkaProps.get("subscribe"), clazz);
    }

    public <T> Dataset<T> readData(String topic, Class<T> clazz)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
            SecurityException, InstantiationException {
        // @formatter:off
        return spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", (String) kafkaProps.get("bootstrap-servers"))
            .option("subscribe", topic)
            .load()
            .select("value")
            .as(Encoders.BINARY())
            .map((MapFunction<byte[], T>) bytes -> {
                Parser<T> parser = (Parser<T>) clazz.getMethod("parser").invoke(null);
                return parser.parseFrom(bytes);
            }, Encoders.kryo(clazz));
        // @formatter:on
    }

    public <T> void writeData(Dataset<T> dataset) throws StreamingQueryException {
        writeData((String) kafkaProps.get("topic"), dataset);
    }

    public <T> void writeData(String topic, Dataset<T> dataset) throws StreamingQueryException {
        // @formatter:off
        StreamingQuery query = dataset
            .map((MapFunction<T, byte[]>) data -> ((GeneratedMessageV3) data).toByteArray(), Encoders.BINARY())
            .writeStream()
            .outputMode("update")
            .format("kafka")
            .option("kafka.bootstrap.servers", (String) kafkaProps.get("bootstrap-servers"))
            .option("topic", topic)
            .option("checkpointLocation", "/tmp/vaquarkhan/checkpoint")
            .start();
        // @formatter:on
        query.awaitTermination();
    }

    public <T> void printData(Dataset<T> dataset) throws StreamingQueryException {
        // @formatter:off
        StreamingQuery query = dataset
            .writeStream()
            .outputMode("update")
            .format("console")
            .start();
        // @formatter:on
        query.awaitTermination();
    }

}
