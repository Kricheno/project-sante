package fr.gouv.data.functions.actions.readers;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class CsvReaderHdfsTestUT {
    private static SparkSession sparkSession;

    private static final Config config = ConfigFactory.load("application.conf");
    private static final String inputPathStr = "file://" + Paths.get(config.getString("app.data.input.path")).toAbsolutePath().toString();

    @BeforeClass
    public static void setUp() {
        sparkSession = SparkSession.builder().master(config.getString("app.master")).appName("test-reader").getOrCreate();
    }

    @Test
    public void testReader() {
        log.info("running testReader");
        log.info("Default spark fileSystem={}", sparkSession.sparkContext().hadoopConfiguration().get("fs.defaultFS"));
        log.info("sparkSession.sparkContext.hadoopConfiguration={}", sparkSession.sparkContext().hadoopConfiguration());
        Dataset<Row> ds = new CsvReader(sparkSession, inputPathStr).get();

        ds.show(5, false);
        ds.printSchema();

        log.info("count_poi={}", ds.select("nombre_aides").distinct().count());

        assertThat(ds.rdd().isEmpty()).isFalse();

    }
}