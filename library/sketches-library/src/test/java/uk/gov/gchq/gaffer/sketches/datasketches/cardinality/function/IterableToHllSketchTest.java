/*
 * Copyright 2021-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.function;

import org.apache.datasketches.hll.HllSketch;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.sketches.datasketches.cardinality.serialisation.json.HllSketchJsonConstants.DEFAULT_LOG_K;

class IterableToHllSketchTest extends FunctionTest<IterableToHllSketch> {

    @Test
    public void shouldCreateEmptyWhenNull() {
        // Given
        final IterableToHllSketch iterableToHllSketch = new IterableToHllSketch();

        // When
        final HllSketch result = iterableToHllSketch.apply(null);

        // Then
        assertThat(result.getEstimate()).isEqualTo(0);
    }

    @Test
    public void shouldCreateHllSketch() {
        // Given
        final IterableToHllSketch iterableToHllSketch = new IterableToHllSketch();
        final List<Object> input = Arrays.asList("one", "two", "three", "four", "five");

        // When
        final HllSketch result = iterableToHllSketch.apply(input);

        // Then
        assertThat(result.getEstimate()).isCloseTo(5, Percentage.withPercentage(0.001));
    }

    @Test
    public void shouldCreateHllSketchCardinality() {
        // Given
        final IterableToHllSketch iterableToHllSketch = new IterableToHllSketch();
        final List<Object> input = Arrays.asList("one", "one", "two", "two", "three");

        // When
        final HllSketch result = iterableToHllSketch.apply(input);

        // Then
        assertThat(result.getEstimate()).isCloseTo(3, Percentage.withPercentage(0.001));
    }

    @Test
    public void shouldSetDefaultLogK() {
        // Given
        final IterableToHllSketch iterableToHllSketch = new IterableToHllSketch();
        final List<Object> input = Arrays.asList("one", "one", "two", "two", "three");

        // When
        final HllSketch result = iterableToHllSketch.apply(input);

        // Then
        assertThat(result.getLgConfigK()).isEqualTo(DEFAULT_LOG_K);
    }

    @Test
    public void shouldCorrectlySetLogK() {
        // Given
        final IterableToHllSketch iterableToHllSketch = new IterableToHllSketch(5);
        final List<Object> input = Arrays.asList("one", "one", "two", "two", "three");

        // When
        final HllSketch result = iterableToHllSketch.apply(input);

        // Then
        assertThat(result.getLgConfigK()).isEqualTo(5);
    }

    @Test
    public void shouldCorrectlyCreateFromAnotherHllSketch() {
        // Given
        final HllSketch anotherSketch = new HllSketch(5);
        IterableToHllSketch iterableToHllSketch = new IterableToHllSketch(anotherSketch);
        final List<Object> input = Arrays.asList("one", "one", "two", "two", "three");

        // When
        final HllSketch result = iterableToHllSketch.apply(input);

        // Then
        assertThat(result.getLgConfigK()).isEqualTo(5);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Iterable.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{HllSketch.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final IterableToHllSketch iterableToHllSketch = new IterableToHllSketch();
        // When
        final String json = new String(JSONSerialiser.serialise(iterableToHllSketch));
        final IterableToHllSketch deserialisedIterableToHllSketch = JSONSerialiser.deserialise(json, IterableToHllSketch.class);
        // Then
        assertEquals(iterableToHllSketch, deserialisedIterableToHllSketch);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.sketches.datasketches.cardinality.function.IterableToHllSketch\"}", json);
    }

    @Override
    protected IterableToHllSketch getInstance() {
        return new IterableToHllSketch();
    }

    @Override
    protected Iterable<IterableToHllSketch> getDifferentInstancesOrNull() {
        return null;
    }
}
