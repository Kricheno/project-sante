package fr.gouv.data.functions.actions.readers;

import fr.gouv.data.DataTest;
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
public class HBaseReader implements Supplier<Dataset<DataTest>> {

    @NonNull
    private final SparkSession sparkSession;

    @Override
    public Dataset<DataTest> get() {

        String catalog = "{\n" +
                "  \"table\": {\n" +
                "    \"namespace\": \"sante\",\n" +
                "    \"name\": \"aides\"\n" +
                "  },\n" +
                "  \"rowkey\": \"key\",\n" +
                "  \"columns\": {\n" +
                "    \"key\": {\n" +
                "      \"cf\": \"rowkey\",\n" +
                "      \"col\": \"key\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    \"nombre_aides\": {\n" +
                "      \"cf\": \"nombre_aides\",\n" +
                "      \"col\": \"nombre_aides\",\n" +
                "      \"type\": \"double\"\n" +
                "    },\n" +
                "    \"montant_total\": {\n" +
                "      \"cf\": \"montant_total\",\n" +
                "      \"col\": \"montant_total\",\n" +
                "      \"type\": \"double\"\n" +
                "    },\n" +
                "    \"libelle_region\": {\n" +
                "      \"cf\": \"loc\",\n" +
                "      \"col\": \"address\",\n" +
                "      \"type\": \"string\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        return sparkSession.read()
                .option(HBaseTableCatalog.tableCatalog(), catalog)
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .load().as(Encoders.bean(DataTest.class));

    }
}
