package fr.gouv.data;

import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import jdk.jfr.internal.tool.Main;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for simple App.
 */
@Slf4j
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        String[] testArgs = new String[]{};
        Main.main(testArgs);
        assertThat(Files.exists(Paths.get(ConfigFactory.load("application.conf").getString("app.data.output.path")))).isTrue();
        assertTrue( true );
    }
}
