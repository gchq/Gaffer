/*
 * Copyright 2021 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
