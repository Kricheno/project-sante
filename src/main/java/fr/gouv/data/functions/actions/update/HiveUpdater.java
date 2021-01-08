package fr.gouv.data.functions.actions.update;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.spark.sql.SparkSession;

import java.util.function.Consumer;

@Slf4j
@AllArgsConstructor
public class HiveUpdater implements Consumer<String> {
    private final String dbName;
    //private final String tableName;
    private static String driverName = "org.apache.hadoop.hive2.jdbc.HiveDriver";
    @NonNull
    private final SparkSession sparkSession;

    @Override
    public void accept(String tableName) {


        String fullHiveTableName = String.format("%s.%s", dbName, tableName);
        String fullHBaseTableName = String.format("%s:%s", dbName, tableName);
        String createDbQuery = String.format(
                "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`%s_ext2` (\n" +
                        "    `key` string COMMENT 'from deserializer',\n"+
                        "    `nombre_aides` double COMMENT 'from deserializer',\n" +
                        "    `montant_total` double COMMENT 'from deserializer',\n"+
                        "    `loc` string COMMENT 'from deserializer'\n" +
                        ")  \n" +
                        "ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'\n"+
                        "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'\n" +
                        "WITH SERDEPROPERTIES (\n" +
                        "'hbase.columns.mapping'=':key,\\nnombre_aides:nombre_aides#b,\\nmontant_total:montant_total#b,\\nloc:libelle_region','serialization.format'='1'\n" +
                        ")\n" +
                        "TBLPROPERTIES (\n" +
                        "    'hbase.table.name'='%s'," +
                        "'hbase.mapred.output.outputtable'='%s'"+
                        ")",
                dbName,tableName,fullHBaseTableName,fullHBaseTableName
        );

        try {
            Class.forName(driverName);
            //Class.forName(JDBC_DRIVER_NAME);
            Connection con = DriverManager.getConnection("jdbc:hive2://localhost:20103/default/test_table");
            Statement stmt = con.createStatement();
            stmt.executeQuery(createDbQuery);
            con.close();

        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        }



        log.info("creating database using createDbQuery={}...", createDbQuery);
        //sparkSession.sql(createDbQuery);


        log.info("c'est terminé");
    }
}
