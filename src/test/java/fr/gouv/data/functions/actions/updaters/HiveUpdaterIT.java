package fr.gouv.data.functions.actions.updaters;


import fr.gouv.data.functions.actions.update.HiveUpdater;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

@Slf4j
public class HiveUpdaterIT {

    private final String dbName = "sante";

    private final String tableName = "aides";

    @Test
    public void testUpdater(){

        SparkSession sparkSession = SparkSession.builder()
                .master("local[2]")
                .appName("test-updater")
                .enableHiveSupport()
                .getOrCreate();

        HiveUpdater hiveUpdater = new HiveUpdater(dbName,sparkSession);
        hiveUpdater.accept(tableName);
        String createDbQuery = String.format("show tables IN %s",dbName);
        sparkSession.sql(createDbQuery).show();


    }
}
