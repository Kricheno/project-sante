package fr.gouv.data.functions.actions.readers;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.gouv.data.HBaseRow;
import fr.gouv.data.functions.actions.writers.HBaseWriter;
import fr.gouv.data.functions.actions.writers.HBaseWriterIT;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;



@Slf4j
public class HBaseReaderIT {
    private static final String catalogName = "sante-catalog.json";

    private static final Config config = ConfigFactory.load("application.conf");
    private static SparkSession sparkSession;

    @BeforeClass
    public static void setUp() {
        sparkSession = SparkSession.builder().master("local[2]").appName("test-reader")
                .getOrCreate();

        List<HBaseRow> expected = Arrays.asList(
                HBaseRow.builder().key("k1").libelle_region("Auvergne-Rhône-Alpes").build(),
                new HBaseRow("key2", 100d, 15000d, "Auvergne-Rhône-Alpes"),
                new HBaseRow("key2", 120d, 20000d, "Bourgogne-Franche-Comté"),
                new HBaseRow("key2", 56d, 10000d, "Auvergne-Rhône-Alpes"),
                new HBaseRow("key2", 324d, 35000d, "Bretagne"),
                new HBaseRow("key2", 128d, 25000d, "Auvergne-Rhône-Alpes")

        );

        Dataset<Row> expectedData = sparkSession.createDataset(expected, Encoders.bean(HBaseRow.class)).toDF();
        expectedData.printSchema();
        expectedData.show();

        new HBaseWriter(catalogName).accept(expectedData);


    }
    @Test
    public void testReader() throws IOException {
        log.info("running hbaseReader test");
        String catalogPathStr = getClass().getClassLoader().getResource(catalogName).getPath();
        log.info("catalogPathStr={}", catalogPathStr);
        String catalog = String.join("\n", Files.readAllLines(Paths.get(catalogPathStr), Charset.defaultCharset()));
        Dataset<HBaseRow> hBaseReader= new HBaseReader(catalog,sparkSession).get();
        log.info("Done!");
        hBaseReader.show(false);
        hBaseReader.printSchema();

        assertThat(hBaseReader.collectAsList().isEmpty()).isFalse();
    }
}
