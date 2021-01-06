package fr.gouv.data;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.gouv.data.functions.actions.readers.CsvReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
@Slf4j
public class App 
{   private static SparkSession sparkSession;
    private static Config config= ConfigFactory.load("application.conf");

    public static void main( String[] args )
    {
        log.info("running Main program");
        CsvReader csvReader=new CsvReader(sparkSession,config.getString("app.data.input.path"));
        System.out.println( "Hello World!" );
    }
}
