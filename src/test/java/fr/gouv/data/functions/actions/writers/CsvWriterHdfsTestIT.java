package fr.gouv.data.functions.actions.writers;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.gouv.data.functions.actions.readers.CsvReader;
import fr.gouv.data.functions.actions.writers.DatasetWriter;
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
public class CsvWriterHdfsTestIT {
    private static SparkSession sparkSession;
    private static final Configuration hadoopConf = new Configuration();

    private static final Config config = ConfigFactory.load("application.conf");
    private static final String outputPathStr = config.getString("app.data.output.path");
    private static final String inputPathStr = config.getString("app.data.input.path");


    private static final Path outputPath = new Path(outputPathStr+"/test.csv");

    private static  Dataset<Row> ds;

    private static FileSystem hdfs;



    private static void clean() throws IOException {
        if(hdfs != null){
            hdfs.delete(outputPath, true);
        }
    }

    @BeforeClass
    public static void setUp() throws IOException {
        sparkSession = SparkSession.builder().master(config.getString("app.master")).appName("test-writer").getOrCreate();

        log.info("Read and save");
        log.info("path:{}",inputPathStr);
        ds = new CsvReader(sparkSession, inputPathStr).get();

        log.info("init hdfs");
        hdfs = FileSystem.get(hadoopConf);
        clean();
        hdfs.mkdirs(outputPath.getParent());
       // hdfs.copyFromLocalFile(outputPath, outputPath);
        //assertThat(hdfs.exists(outputPath)).isTrue();
       // assertThat(hdfs.listFiles(outputPath, true).hasNext()).isTrue();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        clean();
    }

    @Test
    public void testWriter() throws IOException {

        log.info("Default hdfs fileSystem={}", hdfs.getConf().get("fs.defaultFS"));
        log.info("Default spark fileSystem={}", sparkSession.sparkContext().hadoopConfiguration().get("fs.defaultFS"));
        log.info("sparkSession.sparkContext.hadoopConfiguration={}", sparkSession.sparkContext().hadoopConfiguration());


       // Dataset<Row> ds = new CsvReader(sparkSession, inputPathStr).get();

        ds.show(5, false);

        DatasetWriter<Row> datasetWriter= new DatasetWriter<>(outputPath.toString());

        Dataset<Row> newDs =ds.select("nombre_aides");
        datasetWriter.accept(newDs);

        Dataset<Row> actualData = sparkSession.
                read().option("delimiter",",").option("header","true").csv(outputPath.toString());


        log.info("written ={}", newDs.count()
        );

        assertThat(actualData.rdd().isEmpty()).isFalse();
        assertThat(ds.rdd().isEmpty()).isFalse();

    }
}