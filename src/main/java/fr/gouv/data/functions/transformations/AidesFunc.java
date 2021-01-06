package fr.gouv.data.functions.transformations;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.function.Function;
@AllArgsConstructor
public class AidesFunc implements Function<Dataset<Row>,String> {
    @Override
    public String apply(Dataset<Row> rowDataset) {
        Dataset<Row> aides= rowDataset.select("nombre_aides","libelle_region")
                .withColumn("nombre_aides",rowDataset.col("nombre_aides").cast("int"))
                .groupBy("libelle_region").agg(functions.sum("nombre_aides")).as("nombre_aides_total");
        aides.show(false);
        String maxAidesRegion = aides.orderBy(functions.col("nombre_aides_total").desc()).select("libelle_region")
                .first().getString(0);
        return maxAidesRegion;
    }
}
