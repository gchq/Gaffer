/*
 * Copyright 2017-2024 Crown Copyright
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.stream.StreamSupplier;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class StreamIterableTest {

    @Test
    void shouldDelegateIteratorToIterable(@Mock final StreamSupplier<Object> streamSupplier,
                                                 @Mock final Stream<Object> stream,
                                                 @Mock final Iterator<Object> iterator) {
        // Given
        given(streamSupplier.get()).willReturn(stream);
        given(stream.iterator()).willReturn(iterator);

        // When
        StreamIterable<Object> wrappedIterable = null;
        Iterator<Object> result = null;
        try {
            wrappedIterable = new StreamIterable<>(streamSupplier);
            result = wrappedIterable.iterator();

            // Then - call has next and check it was called on the mock.
            result.hasNext();
            verify(iterator).hasNext();
        } finally {
            CloseableUtil.close(result, wrappedIterable);
        }
    }

    @Test
    void shouldDelegateCloseToStreamIterable(@Mock final StreamSupplier<Object> streamSupplier)
            throws IOException {
        // Given
        final StreamIterable<Object> streamIterable = new StreamIterable<>(streamSupplier);

        // When
        streamIterable.close();

        // Then
        verify(streamSupplier).close();
    }
}
