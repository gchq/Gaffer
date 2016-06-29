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

/**
 * A <code>FetchExport</code> fetches an export {@link Iterable} for a
 * provided key. If a key is not provided the default key is 'ALL'.
 *
 * @see UpdateExport
 * @see FetchExporter
 */
public class FetchExport extends ExportOperation<Void, CloseableIterable<?>> {
    private int start = 0;
    private int end = Integer.MAX_VALUE;

    /**
     * Constructs an <code>FetchExport</code> with the key set to 'ALL'.
     */
    public FetchExport() {
        super();
    }

    /**
     * Constructs an <code>FetchExport</code> with the provided key.
     *
     * @param key the key to use to fetch results from the export.
     */
    public FetchExport(final String key) {
        super(key);
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

    public static class Builder extends ExportOperation.Builder<FetchExport, Void, CloseableIterable<?>> {
        public Builder() {
            super(new FetchExport());
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
        public Builder key(final String key) {
            return (Builder) super.key(key);
        }

        @Override
        public Builder option(final String name, final String value) {
            return (Builder) super.option(name, value);
        }
    }
}
