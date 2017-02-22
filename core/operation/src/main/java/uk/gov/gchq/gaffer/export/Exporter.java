/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.export;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.export.ExportOperation;
import uk.gov.gchq.gaffer.operation.impl.export.initialise.InitialiseExport;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.gaffer.util.ExportUtil;

/**
 * An <code>Exporter</code> can store data of any kind and retrieve it with
 * pagination.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
public abstract class Exporter<CONFIG, INITIALISE_EXPORT extends InitialiseExport> {
    public static final String SEPARATOR = "_";
    private User user;
    private String plainTextUserId;
    private Long timestamp = System.currentTimeMillis();
    private String key = ExportOperation.DEFAULT_KEY;
    private String exportName;

    /**
     * Initialises the export. This base method just stores the current user.
     * Override this method (and call super) to add further initialisation.
     *
     * @param initialiseExport the initialise export operation
     * @param config           configuration for the export (This will be an instance of a gaffer Store)
     * @param user             the user who initiated the export
     * @param executionId      the execution id
     */
    public void initialise(final INITIALISE_EXPORT initialiseExport, final CONFIG config, final User user, final String executionId) {
        setKey(initialiseExport.getKey());
        setUser(user);
        this.exportName = null != initialiseExport.getExportName() ? initialiseExport.getExportName() : executionId;
    }

    public final void add(final Iterable<?> values, final User user, final String executionId) throws OperationException {
        validateSameUser(user);
        _add(values, user);
    }

    public final CloseableIterable<?> get(final User user, final int start, final int end) throws OperationException {
        validateSameUser(user);
        return _get(user, start, end);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    public String getKey() {
        return key;
    }

    protected User getUser() {
        return user;
    }

    protected String getPlainTextUserId() {
        return plainTextUserId;
    }

    public String getExportName() {
        return exportName;
    }

    protected abstract void _add(final Iterable<?> values, final User user) throws OperationException;

    protected abstract CloseableIterable<?> _get(final User user, final int start, final int end) throws OperationException;

    private void validateSameUser(final User user) {
        if (null == this.user) {
            throw new IllegalArgumentException("This exporter cannot be used until it has been initialised by a user.");
        } else if (null == user) {
            throw new IllegalArgumentException("A user is required to use this Exporter.");
        } else if (!this.user.equals(user)) {
            throw new IllegalArgumentException("This Exporter has been initialised by user " + this.user.getUserId() + ". It cannot be used a different user, " + user.getUserId() + ".");
        }
    }

    private void setUser(final User user) {
        this.user = user;
        plainTextUserId = StringUtil.getPlainText(user.getUserId());
    }

    private void setKey(final String key) {
        ExportUtil.validateKey(key);
        this.key = key;
    }

    @JsonGetter("class")
    String getClassName() {
        return getClass().getName();
    }

    @JsonSetter("class")
    void setClassName(final String className) {
        // ignore the className as it will be picked up by the JsonTypeInfo annotation.
    }
}
