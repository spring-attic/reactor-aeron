package reactor.ipc.aeron;

import org.junit.Test;

import java.time.Duration;

/**
 * @author Anatoly Kadyshev
 */
public class DriverManagerTest {

    @Test
    public void test() throws InterruptedException {
        DriverManager driverManager = new DriverManager();
        driverManager.launchDriver();

        driverManager.getAeronCounters().forEach((id, label) -> {
            System.out.println(id + ", " + label);
        });

        driverManager.shutdownDriver().block(Duration.ofSeconds(5));
    }

}