package reactor.aeron;

public class DebugUtil {

  public static void log(String msg) {
    System.out.println("[" + Thread.currentThread().getName() + "] - " + msg);
  }
}
