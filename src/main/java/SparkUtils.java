import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {

    public static SparkSession getSparkSession() {
        SparkConf conf = new SparkConf()
                .setAppName("testApp")
                .setMaster("local[1]");
        //SparkContext sc = new SparkContext(conf);

        return SparkSession.builder().config(conf).getOrCreate();
    }

    public static boolean isDebugMode(){
     return java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().toString().
             indexOf("-agentlib:jdwp") > 0;
    }
}
