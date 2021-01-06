package fr.gouv.data.functions.actions.readers;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.BeforeClass;
import org.junit.Test;



@Slf4j
public class CsvReaderTest {
    private static Config config = ConfigFactory.load("application.conf");
    private static SparkSession sparkSession;
    @BeforeClass
    public static void setUp(){
        log.info("initializing sparksession");
        sparkSession = SparkSession.builder().master(config.getString("app.master"))
                .appName(config.getString("app.name")).getOrCreate();
    }
    @Test
    public void readerTest(){
        String inputPath = "target/test-classes/fonds-solidarite-volet-1-regional-naf-latest.csv";
        Dataset<Row> ds = new CsvReader(sparkSession,inputPath).get();
        log.info("5 rows of file");
        ds.show(5,false);
        log.info("describe");
       // ds.describe(functions.col("nombre_aides"));

    }
}