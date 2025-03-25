import java.util.concurrent.LinkedBlockingQueue
import grails.util.Holders as CH

class BackgroundThreadGrailsPlugin {
    def version = 1.7
    def dependsOn = [:]

    // TODO Fill in these fields
    def author = "Robert Fischer"
    def authorEmail = "robert.fischer@smokejumperit.com"
    def title = "Background Thread Plugin"
    def description = '''\
Provides the ability to launch background threads by passing closures into a service method.
'''
    def pluginExcludes = [
        "grails-app/domain/**",
    ]
    
    // URL to the plugin's documentation
    def documentation = 'http://grails.org/BackgroundThread+Plugin'

    def doWithSpring = {
      bgExceptionHandler(BackgroundExceptionHandler) {}

      bgThreadFactory(DefaultThreadFactory) {
          exceptionHandler = bgExceptionHandler
      }

      bgQueue(LinkedBlockingQueue, CH.config.backgroundThread.queueSize ?: 1000) {}

      bgThreadManager(BackgroundThreadManager) { bean ->
        bean.initMethod = 'start'
        bean.destroyMethod = 'stop'
        bean.autowire = 'byName'
        threadFactory = bgThreadFactory
        queue = bgQueue
        threadCount = CH.config.backgroundThread.threadCount ?: 5
        tasksPerDrain = CH.config.backgroundThread.tasksPerDrain ?: 100
        sessionFactory = ref("sessionFactory")
      }

    }
   
    def doWithApplicationContext = { applicationContext ->
        // TODO Implement post initialization spring config (optional)      
    }

    def doWithWebDescriptor = { xml ->
        // TODO Implement additions to web.xml (optional)
    }
                                          
    def doWithDynamicMethods = { ctx ->
        // TODO Implement registering dynamic methods to classes (optional)
    }
    
    def onChange = { event ->
        // TODO Implement code that is executed when any artefact that this plugin is
        // watching is modified and reloaded. The event contains: event.source,
        // event.application, event.manager, event.ctx, and event.plugin.
    }

    def onConfigChange = { event ->
        // TODO Implement code that is executed when the project configuration changes.
        // The event is the same as for 'onChange'.
    }
}
