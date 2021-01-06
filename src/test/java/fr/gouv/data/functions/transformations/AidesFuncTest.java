package fr.gouv.data.functions.transformations;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.gouv.data.functions.actions.readers.CsvReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;

@Slf4j
public class AidesFuncTest {
    public static SparkSession sparkSession;
    private static Config config = ConfigFactory.load("application.conf");
    @BeforeClass
    public static void setUp(){
        log.info("initializing sparksession");
        sparkSession = SparkSession.builder().master(config.getString("app.master"))
                .appName(config.getString("app.name")).getOrCreate();
    }
    @Test
    public void aidesTest(){
        log.info("reading from csv file ...");
        Dataset<Row> ds= new CsvReader(sparkSession,config.getString("app.data.input.path")).get();
        log.info("calculating max ...");
        String result= new AidesFunc().apply(ds);
        log.info("max is={}",result);
        //assertThat(result).isEqualTo();

    }

}