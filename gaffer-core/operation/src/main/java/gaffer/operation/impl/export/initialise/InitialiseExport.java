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

package gaffer.operation.impl.export.initialise;

import com.fasterxml.jackson.annotation.JsonIgnore;
import gaffer.export.Exporter;
import gaffer.operation.AbstractOperation;
import gaffer.operation.impl.export.ExportOperation;
import gaffer.util.ExportUtil;

/**
 * A <code>UpdateExport</code> allows the results of a previous operation in an
 * {@link gaffer.operation.OperationChain} to be added to an export, keyed on
 * an optional key. If a key is not provided the default key is 'ALL'.
 *
 * @see gaffer.operation.impl.export.UpdateExport
 * @see gaffer.operation.impl.export.FetchExport
 * @see gaffer.operation.impl.export.FetchExporter
 * @see gaffer.operation.impl.export.FetchExporters
 */
public abstract class InitialiseExport extends AbstractOperation<Object, Object> {
    private final Exporter exporter;
    private String key;

    public InitialiseExport(final Exporter exporter) {
        this(exporter, ExportOperation.DEFAULT_KEY);
    }

    public InitialiseExport(final Exporter exporter, final String key) {
        this.exporter = exporter;
        setKey(key);
    }

    public String getKey() {
        return key;
    }

    public void setKey(final String key) {
        ExportUtil.validateKey(key);
        this.key = key;
    }

    @JsonIgnore
    public Exporter getExporter() {
        return exporter;
    }

    public long getTimestamp() {
        return exporter.getTimestamp();
    }

    public void setTimestamp(final long timestamp) {
        exporter.setTimestamp(timestamp);
    }

    public static class Builder<OP_TYPE extends InitialiseExport> extends AbstractOperation.Builder<OP_TYPE, Object, Object> {
        protected Builder(final OP_TYPE initialiseExport) {
            super(initialiseExport);
        }

        public Builder<OP_TYPE> timestamp(final long timestamp) {
            getOp().setTimestamp(timestamp);
            return this;
        }

        public Builder key(final String key) {
            getOp().setKey(key);
            return this;
        }

        @Override
        public Builder<OP_TYPE> option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
