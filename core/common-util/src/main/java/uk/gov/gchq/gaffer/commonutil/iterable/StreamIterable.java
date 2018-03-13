/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil.iterable;

import com.fasterxml.jackson.annotation.JsonIgnore;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.stream.StreamSupplier;

import java.util.stream.Stream;

/**
 * A {@link uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable} which uses
 * a {@link java.util.stream.Stream} as the underlying data source.
 *
 * @param <T> the object type
 */
public class StreamIterable<T> implements CloseableIterable<T> {
    private final StreamSupplier<T> streamSupplier;

    public StreamIterable(final StreamSupplier<T> streamSupplier) {
        this.streamSupplier = streamSupplier;
    }

    @Override
    public void close() {
        CloseableUtil.close(streamSupplier);
    }

    @Override
    public CloseableIterator<T> iterator() {
        return new StreamIterator<>(streamSupplier.get());
    }

    /**
     * Get a {@link java.util.stream.Stream} from the {@link uk.gov.gchq.gaffer.commonutil.stream.StreamSupplier}.
     * <p>
     * This enables the creation of multiple stream objects from the same base
     * data, without operating on the same stream multiple times.
     *
     * @return a new {@link java.util.stream.Stream}
     */
    @JsonIgnore
    public Stream<T> getStream() {
        return streamSupplier.get();
    }
}
