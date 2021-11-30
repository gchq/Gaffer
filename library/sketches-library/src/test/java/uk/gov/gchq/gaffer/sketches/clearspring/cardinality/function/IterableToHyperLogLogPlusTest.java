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

package uk.gov.gchq.gaffer.sketches.clearspring.cardinality.function;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class IterableToHyperLogLogPlusTest extends FunctionTest {
    @Test
    public void shouldCreateEmptyWhenNull() {
        //Given
        IterableToHyperLogLogPlus iterableToHyperLogLogPlus = new IterableToHyperLogLogPlus();

        //When
        HyperLogLogPlus result = iterableToHyperLogLogPlus.apply(null);

        //Then
        assertThat(result.cardinality()).isEqualTo(0);
    }

    @Test
    public void shouldCreateHllSketch() {
        //Given
        IterableToHyperLogLogPlus iterableToHyperLogLogPlus = new IterableToHyperLogLogPlus();
        List<Object> input = Arrays.asList("one", "two", "three", "four", "five");

        //When
        HyperLogLogPlus result = iterableToHyperLogLogPlus.apply(input);

        //Then
        assertThat(result.cardinality()).isEqualTo(5);
    }

    @Test
    public void shouldCreateHllSketchCardinality() {
        //Given
        IterableToHyperLogLogPlus iterableToHyperLogLogPlus = new IterableToHyperLogLogPlus();
        List<Object> input = Arrays.asList("one", "one", "two", "two", "three");

        //When
        HyperLogLogPlus result = iterableToHyperLogLogPlus.apply(input);

        //Then
        assertThat(result.cardinality()).isEqualTo(3);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Iterable.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{HyperLogLogPlus.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final IterableToHyperLogLogPlus iterableToHyperLogLogPlus = new IterableToHyperLogLogPlus();
        // When
        final String json = new String(JSONSerialiser.serialise(iterableToHyperLogLogPlus));
        IterableToHyperLogLogPlus deserialisedIterableToHyperLogLogPlus = JSONSerialiser.deserialise(json, IterableToHyperLogLogPlus.class);
        // Then
        assertEquals(iterableToHyperLogLogPlus, deserialisedIterableToHyperLogLogPlus);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.sketches.clearspring.cardinality.function.IterableToHyperLogLogPlus\"}", json);
    }

    @Override
    protected IterableToHyperLogLogPlus getInstance() {
        return new IterableToHyperLogLogPlus();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}
