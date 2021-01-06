package fr.gouv.data.functions.actions.readers;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.gouv.data.functions.actions.readers.CsvReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class CsvReaderHdfsIT {

    private static SparkSession sparkSession;
    private static final Configuration hadoopConf = new Configuration();

    private static final Config config = ConfigFactory.load("application.conf");
    private static final String inputPathStr = config.getString("app.data.output.path");

    private static final Path inputPath = new Path(inputPathStr);

    private static FileSystem hdfs;

    private static void clean() throws IOException {
        if(hdfs != null){
            hdfs.delete(inputPath, true);
        }
    }

    @BeforeClass
    public static void setUp() throws IOException {
        log.info("init hdfs");
        hdfs = FileSystem.get(hadoopConf);
        clean();
        hdfs.mkdirs(inputPath.getParent());
        hdfs.copyFromLocalFile(inputPath, inputPath);
        assertThat(hdfs.exists(inputPath)).isTrue();
        assertThat(hdfs.listFiles(inputPath, true).hasNext()).isTrue();
        sparkSession = SparkSession.builder().master(config.getString("app.master")).appName("test-reader").getOrCreate();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        clean();
    }

    @Test
    public void testReader(){
        log.info("running DataTourismReaderWithHadoopUnitUT.testReader");
        log.info("Default hdfs fileSystem={}", hdfs.getConf().get("fs.defaultFS"));
        log.info("Default spark fileSystem={}", sparkSession.sparkContext().hadoopConfiguration().get("fs.defaultFS"));
        log.info("sparkSession.sparkContext.hadoopConfiguration={}", sparkSession.sparkContext().hadoopConfiguration());
        Dataset<Row> ds = new CsvReader(sparkSession, inputPathStr).get();

        ds.show(5, false);
        ds.printSchema();

        log.info("nombre d'aides ={}", ds.select("nombre_aides").distinct().count()
        );

        assertThat(ds.rdd().isEmpty()).isFalse();

    }
}
