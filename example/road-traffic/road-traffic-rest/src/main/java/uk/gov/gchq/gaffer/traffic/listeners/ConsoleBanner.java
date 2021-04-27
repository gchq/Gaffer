package uk.gov.gchq.gaffer.traffic.listeners;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.logging.Logger;

/**
 * A {@link ServletContextListener}  to write a message to the logger once the application is ready.
 */
public class ConsoleBanner implements ServletContextListener {

    private static final Logger LOGGER = Logger.getLogger(ConsoleBanner.class.getName());

    @Override
    public void contextInitialized(final ServletContextEvent servletContextEvent) {
        final String port = System.getProperty("gaffer.rest-api.port", "8080");
        final String path = System.getProperty("gaffer.rest-api.basePath", "rest");

        LOGGER.info(String.format("Gaffer road-traffic example is ready at: http:/localhost:%s/%s", port, path));
    }

    @Override
    public void contextDestroyed(final ServletContextEvent servletContextEvent) {
        // Empty
    }
}
