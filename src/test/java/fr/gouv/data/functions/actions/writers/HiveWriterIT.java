package fr.gouv.data.functions.actions.writers;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class HiveWriterIT {

    private final String dbName = "sante";

    private final String tableName = "aides";

    private final String location = "/tmp/hive/warehouse";


    @Test
    public void testWriter(){
        log.info("running hiveWriter test");
        SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("test-writer")
                .enableHiveSupport()
                .getOrCreate();

        List<HBaseRow> expected = Arrays.asList(
                HBaseRow.builder().key("k1").libelle_region("Bordeaux").nombre_aides(50d).montant_total(20000d).build(),
                new HBaseRow("key2", 100d, 200000d, "Paris"),
                new HBaseRow("key2", 200d, 150000d, "Toulouse")

        );

        Dataset<Row> expectedData = sparkSession.createDataset(expected, Encoders.bean(HBaseRow.class)).toDF();
        expectedData.printSchema();
        expectedData.show();

        log.info("showing databases before...");
        sparkSession.sql("show databases").show();

        new HiveWriter(dbName, tableName, location).accept(expectedData);

        log.info("showing databases after...");
        sparkSession.sql("show databases").show();

        Dataset<HBaseRow> actualData = sparkSession.sql(String.format("SELECT * from %s.%s", dbName, tableName))
                .as(Encoders.bean(HBaseRow.class));

        log.info("daaaaata {}",actualData.collectAsList());
        assertThat(actualData.collectAsList()).containsExactlyInAnyOrder(expected.toArray(new HBaseRow[0]));
    }

    @Data @Builder @AllArgsConstructor @NoArgsConstructor
    public static class HBaseRow implements Serializable {
        private String key;
        private Double nombre_aides;
        private Double montant_total;
        private String libelle_region;

    }
}