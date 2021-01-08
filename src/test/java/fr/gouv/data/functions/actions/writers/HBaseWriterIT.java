package fr.gouv.data.functions.actions.writers;

import fr.gouv.data.DataTest;
import fr.gouv.data.HBaseRow;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class HBaseWriterIT {

    private final String catalogName = "sante-catalog.json";

    private final String catalog = "{\n" +
            "  \"table\": {\n" +
            "    \"namespace\": \"sante\",\n" +
            "    \"name\": \"aides\"\n" +
            "  },\n" +
            "  \"rowkey\": \"key\",\n" +
            "  \"columns\": {\n" +
            "    \"key\": {\n" +
            "      \"cf\": \"rowkey\",\n" +
            "      \"col\": \"key\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"nombre_aides\": {\n" +
            "      \"cf\": \"nombre_aides\",\n" +
            "      \"col\": \"nombre_aides\",\n" +
            "      \"type\": \"double\"\n" +
            "    },\n" +
            "    \"montant_total\": {\n" +
            "      \"cf\": \"montant_total\",\n" +
            "      \"col\": \"montant_total\",\n" +
            "      \"type\": \"double\"\n" +
            "    },\n" +
            "    \"libelle_region\": {\n" +
            "      \"cf\": \"libelle_region\",\n" +
            "      \"col\": \"libelle_region\",\n" +
            "      \"type\": \"string\"\n" +
            "    }\n" +
            "  }\n" +
            "}";

    @Test
    public void testWriter(){
        log.info("running hbaseWriter test");
        SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("test-writer")
                .getOrCreate();

        List<HBaseRow> expected = Arrays.asList(
                HBaseRow.builder().key("key1").nombre_aides(325d).montant_total(5888d).libelle_region("Auvergne-Rhône-Alpes").build(),
                new HBaseRow("key2", 100d, 15000d, "Auvergne-Rhône-Alpes"),
                new HBaseRow("key2", 120d, 2000d, "Bourgogne-Franche-Comté"),
                new HBaseRow("key2", 5d, 10000d, "Auvergne-Rhône-Alpes"),
                new HBaseRow("key3", 324d, 35000d, "Paris"),
                new HBaseRow("key4", 250d, 8500d, "Bordeaux"),
                new HBaseRow("key4", 128d, 25000d, "Bretagne")

        );

        Dataset<Row> expectedData = sparkSession.createDataset(expected, Encoders.bean(HBaseRow.class)).toDF();
        expectedData.printSchema();
        expectedData.show();

        new HBaseWriter(catalogName).accept(expectedData);

        Dataset<DataTest> actualData = sparkSession.read()
                .option(HBaseTableCatalog.tableCatalog(), catalog)
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .load().as(Encoders.bean(DataTest.class));

        //   assertThat(actualData.collectAsList()).containsExactlyElementsOf(expected);
    }


}
