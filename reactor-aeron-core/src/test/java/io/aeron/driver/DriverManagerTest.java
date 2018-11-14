package io.aeron.driver;

import java.time.Duration;
import org.junit.jupiter.api.Test;

public class DriverManagerTest {

  @Test
  public void test() throws InterruptedException {
    DriverManager driverManager = new DriverManager();
    driverManager.launchDriver();

    driverManager
        .getAeronCounters()
        .forEach(
            (id, label) -> {
              System.out.println(id + ", " + label);
            });

    driverManager.shutdownDriver().block(Duration.ofSeconds(5));
  }
}
