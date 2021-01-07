package fr.gouv.data.functions.actions.readers;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.function.Supplier;
@Slf4j
@AllArgsConstructor
public class HiveReader implements Supplier<Dataset<Row>> {
    private final String dbName;
    private final String tableName;

    @NonNull
    private final SparkSession sparkSession;

    @Override
    public Dataset<Row> get() {

        String fullTableName = String.format("%s.%s", dbName, tableName);

        //try {
//
//            FileSystem hdfs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
//
//            Path fullDbPath = new Path(fullDbDataPathStr);
//            if (hdfs.exists(fullDbPath)) {
//                hdfs.delete(fullDbPath, true);
//            }
//            hdfs.mkdirs(fullDbPath);
            String createDbQuery = String.format("SELECT * FROM %s", fullTableName);
            log.info("select data using createDbQuery={}...", createDbQuery);
            sparkSession.sql(createDbQuery);

            log.info("reading data into hive table = {}...", fullTableName);
            log.info("data: {}", sparkSession.sql(createDbQuery));

//        } catch (IOException ioException) {
//            log.error("could not create write data into hive due to ...", ioException);
//        }
        log.info("done!");
        return sparkSession.sql(String.format("SELECT * from %s.%s", dbName, tableName));
    }
}
