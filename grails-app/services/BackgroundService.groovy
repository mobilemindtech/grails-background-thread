class BackgroundService {

    boolean transactional = false

    def bgThreadManager

    def execute(String name, Closure toRun) {
      if(!bgThreadManager) {
        throw new IllegalStateException("Need a background thread manager")
      }
      Runnable runMe = {
        log.debug("Executing ${name}")
        toRun()
        log.debug("Done executing ${name}")
      }
      bgThreadManager.queueRunnable(runMe);
    }

}
