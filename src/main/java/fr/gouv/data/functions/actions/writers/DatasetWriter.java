package fr.gouv.data.functions.actions.writers;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

import java.util.function.Consumer;
@Slf4j
@AllArgsConstructor
public class DatasetWriter<T> implements Consumer<Dataset<T>> {
    public String outputPathStr ;
    @Override
    public void accept(Dataset<T> tDataset) {
        Dataset<T> ds = tDataset.cache();
        String finalOutputPathStr = String.format("%s_%d", outputPathStr, System.currentTimeMillis());
        log.info("writing ds.count()={} in outputFile={} ",ds.count(),finalOutputPathStr);
        ds.printSchema();
        ds.show(5);
        ds.write().mode(SaveMode.ErrorIfExists).option("header",true).format("csv").save(outputPathStr);
        ds.unpersist();
    }
}
