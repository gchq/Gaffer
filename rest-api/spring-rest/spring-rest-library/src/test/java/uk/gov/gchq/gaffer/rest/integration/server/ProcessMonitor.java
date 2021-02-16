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

package uk.gov.gchq.gaffer.rest.integration.server;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Scanner;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ProcessMonitor implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessMonitor.class);
    private static final boolean ERROR_LOGGING = true;
    private static final boolean INFO_LOGGING = false;

    private final Process process;
    private final Scanner scanner;
    private final boolean errorLogging;

    public static void monitorInputStream(final Process process) {
        new ProcessMonitor(process, process.getInputStream(), INFO_LOGGING).start();
    }

    public static void monitorErrorStream(final Process process) {
        new ProcessMonitor(process, process.getErrorStream(), ERROR_LOGGING).start();
    }

    private ProcessMonitor(final Process process, final InputStream inputStream, final boolean errorLogging) {
        this.process = process;
        this.scanner = new Scanner(new InputStreamReader(inputStream, UTF_8));
        this.errorLogging = errorLogging;
    }

    private void start() {
        new Thread(this).start();
    }

    public void run() {
        while (process.isAlive()) {
            readLine();
        }
    }

    private void readLine() {
        if (scanner.hasNext()) {
            final String line = scanner.nextLine();
            if (!StringUtils.isEmpty(line)) {
                if (errorLogging) {
                    LOGGER.error(line);
                } else {
                    LOGGER.info(line);
                }
            }
        }
    }
}
