package fr.gouv.data.functions.actions.readers;

import fr.gouv.data.functions.actions.writers.HiveWriter;
import fr.gouv.data.functions.actions.writers.HiveWriterIT;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class HiveReaderIT {
    private final String dbName = "sante";

    private final String tableName = "aides";

    @Test
    public void testReader(){
        log.info("running hiveWriter test");
        SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("test-writer")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> actualData=new HiveReader(dbName, tableName,sparkSession).get();
        log.info("data: {}",actualData.collectAsList());
        actualData.show();

        assertThat(actualData.rdd().isEmpty()).isFalse();


    }
}
