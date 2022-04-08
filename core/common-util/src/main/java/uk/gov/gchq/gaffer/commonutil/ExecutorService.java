/*
 * Copyright 2019-2020 Crown Copyright
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
package uk.gov.gchq.gaffer.commonutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * An {@code ExecutorService} that can schedule commands to run after a given
 * delay, or to execute periodically.
 *
 * @see java.util.concurrent.ScheduledExecutorService
 **/
public final class ExecutorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorService.class);
    private static ScheduledExecutorService service;

    private ExecutorService() {
        // private constructor to prevent instantiation
    }

    public static synchronized void initialise(final int jobExecutorThreadCount) {
        if (service == null) {
            LOGGER.debug("Initialising ExecutorService with " + jobExecutorThreadCount + " threads");
            service = Executors.newScheduledThreadPool(jobExecutorThreadCount, runnable -> {
                final Thread thread = new Thread(runnable);
                thread.setDaemon(true);
                return thread;
            });
        }
    }

    public static ScheduledExecutorService getService() {
        return service;
    }

    public static boolean isEnabled() {
        return null != service;
    }

    public static void shutdown() {
        if (null != service) {
            service.shutdown();
        }
        service = null;
    }
}
