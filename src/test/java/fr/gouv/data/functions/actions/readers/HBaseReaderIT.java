package fr.gouv.data.functions.actions.readers;

import fr.gouv.data.DataTest;
import fr.gouv.data.functions.actions.writers.HBaseWriter;
import fr.gouv.data.functions.actions.writers.HBaseWriterIT;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableScanRDD;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class HBaseReaderIT {


    private static SparkSession sparkSession ;
    private static String catalogName = "sante-catalog.json";
    private static List<DataTest> expected = Arrays.asList(
            DataTest.builder().key("k1").libelle_region("Auvergne-Rhône-Alpes").build(),
            new DataTest("key2", 100d, 15000d, "Auvergne-Rhône-Alpes"),
            new DataTest("key2", 120d, 20000d, "Bourgogne-Franche-Comté"),
            new DataTest("key2", 56d, 10000d, "Auvergne-Rhône-Alpes"),
            new DataTest("key4", 324d, 3500d, "Bretagne"),
            new DataTest("key2", 128d, 25000d, "Auvergne-Rhône-Alpes")

    );

    @BeforeClass
    public static void addDataHBase(){
        sparkSession=SparkSession
                .builder()
                .master("local[2]")
                .appName("test-reader")
                .getOrCreate();

        Dataset<Row> expectedData = sparkSession.createDataset(expected, Encoders.bean(DataTest.class)).toDF();
        expectedData.printSchema();
        expectedData.show();

        new HBaseWriter(catalogName).accept(expectedData);

    }

    @Test
    public void testReader(){

        Dataset<DataTest> ds = new HBaseReader(sparkSession).get();
        ds.show(false);

    }
}
