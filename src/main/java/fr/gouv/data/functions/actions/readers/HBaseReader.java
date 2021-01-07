package fr.gouv.data.functions.actions.readers;

import fr.gouv.data.DataTest;
import fr.gouv.data.HBaseRow;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import java.util.function.Supplier;

@Slf4j @AllArgsConstructor
public class HBaseReader implements Supplier<Dataset<HBaseRow>> {

    @NonNull
    private String catalog;
    @NonNull
    private SparkSession sparkSession ;


    @Override
    public Dataset<HBaseRow> get() {
        log.info("Reading from hbase ..");
//        Boolean stopped= !sparkSession.sparkContext().isStopped();
//    try{
//        if(stopped){
        Dataset<HBaseRow> data = sparkSession.read()
                .option(HBaseTableCatalog.tableCatalog(), catalog)
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .load().as(Encoders.bean(HBaseRow.class));
        return data;
//        }
//    } catch (Exception e) {
//        log.info("cannot read from hbase due to this error: ",e);
//    }
//    }
//    return sparkSession.(Encoders.bean(HBaseRow.class));


    }
}
