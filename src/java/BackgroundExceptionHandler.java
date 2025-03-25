import grails.util.GrailsUtil;
import org.apache.log4j.*;

public class BackgroundExceptionHandler implements Thread.UncaughtExceptionHandler {

  private static final Logger log = Logger.getLogger(BackgroundExceptionHandler.class);

  public void uncaughtException(final Thread t, final Throwable e) {
    try {
        //todo shouldn't we use the sanitized value in the log message instead?
        GrailsUtil.deepSanitize(e);
        log.error("Unhandled failure while processing " + t.getName(), e);
    } catch(Exception e2) {
      // Well, punt
      System.err.println(e.toString());
      System.err.println(e2.toString());
    }
  }

}
