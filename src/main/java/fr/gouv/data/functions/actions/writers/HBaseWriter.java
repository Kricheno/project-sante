package fr.gouv.data.functions.actions.writers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class HBaseWriter implements Consumer<Dataset<Row>> {
    private final String catalogName;

    @Override
    public void accept(Dataset<Row> rowDataset) {
        log.info("Reading catalog file={}", catalogName);

        try {
            String catalogPathStr = getClass().getClassLoader().getResource(catalogName).getPath();
            log.info("catalogPathStr={}", catalogPathStr);
            String catalog = String.join("\n", Files.readAllLines(Paths.get(catalogPathStr), Charset.defaultCharset()));
            log.info("catalog=\n{}", catalog);
            log.info("writing data into hbase...");

            rowDataset
                    .write()
                    .mode("append")
                    .option(HBaseTableCatalog.tableCatalog(), catalog).option(HBaseTableCatalog.newTable(), "5")
                    .format("org.apache.spark.sql.execution.datasources.hbase")
                    .save();

            log.info("done!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }
}
