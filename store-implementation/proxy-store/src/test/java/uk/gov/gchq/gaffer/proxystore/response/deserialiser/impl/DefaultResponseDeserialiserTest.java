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
package uk.gov.gchq.gaffer.proxystore.response.deserialiser.impl;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.TypeReferenceStoreImpl;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultResponseDeserialiserTest {

    @Test
    public void shouldThrowSerialisationExceptionWhenResponseIsInvalid() {
        assertThatExceptionOfType(SerialisationException.class).isThrownBy(() -> new DefaultResponseDeserialiser<>(new TypeReferenceStoreImpl.StoreTraits()).deserialise("[\"JUNK_TRAIT\"]"));
        assertThatExceptionOfType(SerialisationException.class).isThrownBy(() -> new DefaultResponseDeserialiser<>(new TypeReferenceStoreImpl.Schema()).deserialise("[\"MATCHED_VERTEX\"]"));
    }

    @Test
    public void shouldDeserialiseValidResponseSuccessfully() throws SerialisationException {
        final String jsonString = "[\n" +
                "  \"MATCHED_VERTEX\",\n" +
                "  \"QUERY_AGGREGATION\"\n" +
                "]";

        final Set<StoreTrait> storeTraits = new DefaultResponseDeserialiser<>(new TypeReferenceStoreImpl.StoreTraits()).deserialise(jsonString);

        final Set<StoreTrait> expectedStoreTraits = new HashSet<>(asList(StoreTrait.MATCHED_VERTEX, StoreTrait.QUERY_AGGREGATION));
        assertEquals(expectedStoreTraits.size(), storeTraits.size());
        assertTrue(expectedStoreTraits.containsAll(storeTraits));
    }

    @Test
    public void shouldDeserialiseValidStringResponseSuccessfully() throws SerialisationException {
        final String jsonString = "Result String";

        final Object result = new DefaultResponseDeserialiser<>(new TypeReferenceImpl.Object()).deserialise(jsonString);

        assertEquals(jsonString, result);
    }
}
