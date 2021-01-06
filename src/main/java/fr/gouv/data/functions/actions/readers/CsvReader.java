package fr.gouv.data.functions.actions.readers;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;
@Slf4j
@AllArgsConstructor
public class CsvReader implements Supplier<Dataset<Row>> {
    @NonNull
    private final SparkSession sparkSession;
    @NonNull
    private final String inputPath;

    @Override
    public Dataset<Row> get() {
        log.info("Reading the file ...");
        Boolean isValidPath= !inputPath.isEmpty() && !sparkSession.sparkContext().isStopped();
        try{
            if(isValidPath){
                return sparkSession.read().option("delimiter",",").option("header","true").csv(inputPath);
            }
        } catch (Exception e) {
            log.info("cannot read file due to this error: ",e);
        }
        return sparkSession.emptyDataset(Encoders.bean(Row.class));
    }
}
