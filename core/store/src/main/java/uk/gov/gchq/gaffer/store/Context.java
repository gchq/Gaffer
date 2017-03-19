/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.store;

import uk.gov.gchq.gaffer.operation.impl.export.Exporter;
import uk.gov.gchq.gaffer.user.User;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A <code>Context</code> contains operation chain execution information, such
 * as the user who executed the operation chain and a map of {@link Exporter}s.
 */
public class Context {
    private final User user;
    private final String jobId;

    /**
     * Map of exporter simple class name to exporter
     */
    private final Map<Class<? extends Exporter>, Exporter> exporters = new HashMap<>();

    public Context() {
        this(new User());
    }

    public Context(final User user) {
        this(user, createJobId());
    }

    public Context(final User user, final String jobId) {
        this.user = user;
        this.jobId = jobId;
    }

    public User getUser() {
        return user;
    }

    public String getJobId() {
        return jobId;
    }

    public Collection<Exporter> getExporters() {
        return Collections.unmodifiableCollection(exporters.values());
    }

    public void addExporter(final Exporter exporter) {
        if (exporters.containsKey(exporter.getClass())) {
            throw new IllegalArgumentException("Exporter of type " + exporter.getClass() + " has already been registered");
        }
        exporters.put(exporter.getClass(), exporter);
    }

    public <E> E getExporter(final Class<? extends E> exporterClass) {
        if (null == exporterClass) {
            throw new IllegalArgumentException("Exporter class is required.");
        }

        final E exporter = (E) exporters.get(exporterClass);
        if (null != exporter) {
            return exporter;
        }

        // Check to see if the class is a subclass of an exporter
        for (final Map.Entry<Class<? extends Exporter>, Exporter> entry : exporters.entrySet()) {
            if (exporterClass.isAssignableFrom(entry.getKey())) {
                return (E) entry.getValue();
            }
        }

        return null;
    }

    public static String createJobId() {
        return UUID.randomUUID().toString();
    }
}
