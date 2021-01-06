package fr.gouv.data.functions.actions.writers;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.gouv.data.functions.actions.readers.CsvReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

@Slf4j
public class DatasetWriterTest {
    private static Config config= ConfigFactory.load("application.conf");
    private static SparkSession sparkSession;

    @BeforeClass
    public static void initializingTest(){
        sparkSession.builder().master(config.getString("app.master")).appName(config.getString("app.name")).getOrCreate();
        log.info("reading a csv");
        Dataset<Row> ds=new CsvReader(sparkSession, config.getString("app.data.input.path")).get();

    }
    @Test
    public void writerTest(){
        Dataset<Row> ds=new CsvReader(sparkSession, config.getString("app.data.input.path")).get();

        DatasetWriter<Row> writer= new DatasetWriter<>(config.getString("app.data.output.path"));
        writer.accept(ds.select());
    }
}