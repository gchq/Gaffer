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

package gaffer.operation.impl.export;

import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.operation.AbstractOperation;

/**
 * A <code>FetchExportResult</code> fetches an export result {@link Iterable} for a
 * provided key. If a key is not provided the default key is 'ALL'.
 *
 * @see UpdateExport
 * @see FetchExport
 */
public class FetchExportResult extends AbstractOperation<Void, CloseableIterable<?>>
        implements ExportOperation<Void, CloseableIterable<?>> {
    private String key;
    private int start = 0;
    private int end = Integer.MAX_VALUE;

    /**
     * Constructs an <code>FetchExportResult</code> with the key set to 'ALL'.
     */
    public FetchExportResult() {
        this(UpdateExport.ALL);
    }

    /**
     * Constructs an <code>FetchExportResult</code> with the provided key.
     *
     * @param key the key to use to fetch results from the export.
     */
    public FetchExportResult(final String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(final String key) {
        this.key = key;
    }

    public int getStart() {
        return start;
    }

    public void setStart(final int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(final int end) {
        this.end = end;
    }

    public static class Builder extends AbstractOperation.Builder<FetchExportResult, Void, CloseableIterable<?>> {
        public Builder() {
            super(new FetchExportResult());
        }

        public Builder key(final String key) {
            getOp().setKey(key);
            return this;
        }

        public Builder start(final int start) {
            getOp().setStart(start);
            return this;
        }

        public Builder end(final int end) {
            getOp().setEnd(end);
            return this;
        }

        @Override
        public Builder option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
