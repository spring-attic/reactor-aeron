package reactor.aeron;

public class TestUtils {

  public static void log(String msg) {
    System.out.println(Thread.currentThread().getName() + " - " + msg);
  }
}
