/*
 * Copyright 2016-2018 Crown Copyright
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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.export.Exporter;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A {@code Context} contains operation chain execution information, such
 * as the user who executed the operation chain and a map of {@link Exporter}s.
 */
public class Context {
    private final User user;
    private final String jobId;
    private final Map<String, Object> config;
    private OperationChain<?> originalOpChain;

    /**
     * Map of exporter simple class name to exporter
     */
    private final Map<Class<? extends Exporter>, Exporter> exporters = new HashMap<>();

    public Context() {
        this(new User());
    }

    public Context(final User user) {
        this(user, new HashMap<>());
    }

    /**
     * Create a new {@link Context} based on the provided context.
     * A shallow clone of the context is carried out and a new job ID is created.
     *
     * @param context the context to shallow clone.
     */
    public Context(final Context context) {
        this(null != context ? context.user : null, null != context ? context.config : null);
        exporters.putAll(context.exporters);
        if (null != context.originalOpChain) {
            originalOpChain = context.originalOpChain.shallowClone();
        }
    }

    private Context(final User user, final Map<String, Object> config) {
        if (null == user) {
            throw new IllegalArgumentException("User is required");
        }
        this.user = user;
        if (null == config) {
            this.config = new HashMap<>();
        } else {
            this.config = config;
        }
        this.jobId = createJobId();
    }

    /**
     * Constructs a context with a provided job ID
     *
     * @param user   the user
     * @param config the config
     * @param jobId  the job ID
     * @deprecated this should not be used. You should let the Context automatically set the job ID.
     */
    @Deprecated
    private Context(final User user, final Map<String, Object> config, final String jobId) {
        if (null == user) {
            throw new IllegalArgumentException("User is required");
        }
        this.user = user;
        if (null == config) {
            this.config = new HashMap<>();
        } else {
            this.config = config;
        }
        if (null == jobId) {
            this.jobId = createJobId();
        } else {
            this.jobId = jobId;
        }
    }

    public User getUser() {
        return user;
    }

    public final String getJobId() {
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

    public Object getConfig(final String key) {
        return config.get(key);
    }

    public void setConfig(final String key, final Object value) {
        config.put(key, value);
    }

    /**
     * Gets the original operation chain. This should not be modified.
     *
     * @return the original operation chain.
     */
    public OperationChain<?> getOriginalOpChain() {
        return originalOpChain;
    }

    public void setOriginalOpChain(final OperationChain<?> originalOpChain) {
        this.originalOpChain = originalOpChain;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        final Context context = (Context) obj;

        return new EqualsBuilder()
                .append(jobId, context.jobId)
                .append(user, context.user)
                .append(originalOpChain, context.originalOpChain)
                .append(exporters, context.exporters)
                .append(config, context.config)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(71, 31)
                .append(jobId)
                .append(user)
                .append(originalOpChain)
                .append(exporters)
                .append(config)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("jobId", jobId)
                .append("user", user)
                .append("originalOpChain", originalOpChain)
                .append("exporters", exporters)
                .append("config", config)
                .toString();
    }

    public static String createJobId() {
        return UUID.randomUUID().toString();
    }

    public static class Builder {
        private User user = new User();
        private final Map<String, Object> config = new HashMap<>();
        private String jobId;

        public Builder user(final User user) {
            this.user = user;
            return this;
        }

        /**
         * Sets the job ID.
         *
         * @param jobId the job ID to set on the context
         * @return the Builder
         * @deprecated this should not be used. You should let the Context automatically set the job ID.
         */
        @Deprecated
        public Builder jobId(final String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder config(final String key, final Object value) {
            this.config.put(key, value);
            return this;
        }

        public Context build() {
            return new Context(user, config, jobId);
        }
    }
}
