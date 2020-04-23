import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class App {

    public static List<String> getDatatoValidate(String filePath) {
        List<String> lines = null;
        File file = new File(filePath);
        try {
            lines = FileUtils.readLines(file, StandardCharsets.UTF_8);
            //lines.forEach(System.out::println);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

    public static void main(String[] args) {
        SparkSession ss = SparkUtils.getSparkSession();
        SparkContext sc = ss.sparkContext();
        //sc.hadoopConfiguration().set(FixedLengthInputFormat.FIXED_RECORD_LENGTH,"8");
        JavaSparkContext jsc = new JavaSparkContext(sc);

        String inputFile = "src/main/resources/test100Rec";

        //FixedLengthInputFormat.class,
        JavaRDD<byte[]> map1 = sc.hadoopFile(inputFile + ".bin", VariableLengthInputFormat.class,
                LongWritable.class, BytesWrapper.class, 2).toJavaRDD().map(tuple -> tuple._2.getBytes());

        List<Partition> partitions = map1.partitions();
        //  JavaRDD<String> stringJavaRDD = jsc.textFile("");
        //  JavaRDD<byte[]> javaRDD = jsc.binaryRecords("src/main/resources/test100Rec.bin", 8);
        // List<byte[]> collect = javaRDD.collect();

        List<byte[]> collected = map1.collect();


        // validate collected data with test with given metadata file
        List<String> dataToCompare = getDatatoValidate(inputFile + "_meta.bin");

        java.util.Iterator<byte[]> iterator = collected.iterator();
        java.util.Iterator<String> metaIter = dataToCompare.iterator();
        assert (collected.size() == dataToCompare.size());
        while (iterator.hasNext()) {
            String[] data = metaIter.next().split("-");
            byte[] arr = iterator.next();

            int length = Integer.parseInt(data[0]) - 4; //-4 for RDW header
            byte[] value = data[1].getBytes();

            assert (arr.length == length);  // compare length of each row
            assert (arr[0] == value[0]);  // compare first byte
            assert (arr[length - 1] == value[0]); // compare last byte
        }
        System.out.println("test");
    }
}
