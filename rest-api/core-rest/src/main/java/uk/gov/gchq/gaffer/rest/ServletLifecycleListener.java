/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Listener for starting and stopping services when the servlet starts up and shuts down.
 * In order to use this listener, reference it in your web.xml file:
 * <pre>
 *     {@code
 *      &lt;xml&gt;
 *          &lt;web-app&gt;
 *              &lt;listener&gt;
 *                 &lt;listener-class&gt;uk.gov.gchq.gaffer.rest.ServletLifecycleListener&lt;/listener-class&gt;
 *              &lt;/listener&gt;
 *          &lt;/web-app&gt;
 *      &lt;/xml&gt;
 *     }
 * </pre>
 */
public class ServletLifecycleListener implements ServletContextListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServletLifecycleListener.class);

    /**
     * Code executed when the servlet starts. The CacheServiceLoader is not initialised here
     * as it requires the store properties file. Therefore it is started when the store
     * is initialised instead.
     * @param servletContextEvent the context event
     */
    @Override
    public void contextInitialized(final ServletContextEvent servletContextEvent) {
    }

    /**
     * Code executed when the servlet is being shut down. The cache service loader is shut
     * down here to avoid ClassNotFoundExceptions which result from a Servlet's ClassLoader
     * being shut down before the ShutdownHooks run. All Gaffer services should use this class
     * rather than Shutdown hooks if they want to run Gaffer in a servlet such as JBOSS or Tomcat.
     * @param servletContextEvent the context event
     */
    @Override
    public void contextDestroyed(final ServletContextEvent servletContextEvent) {
        LOGGER.info("Server shutting down - releasing resources");
        CacheServiceLoader.shutdown();
    }
}
