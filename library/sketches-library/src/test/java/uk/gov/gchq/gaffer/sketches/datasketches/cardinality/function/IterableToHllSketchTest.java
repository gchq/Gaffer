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

package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.function;

import com.yahoo.sketches.hll.HllSketch;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class IterableToHllSketchTest extends FunctionTest {

    @Test
    public void shouldCreateEmptyWhenNull() {
        //Given
        IterableToHllSketch iterableToHllSketch = new IterableToHllSketch();

        //When
        HllSketch result = iterableToHllSketch.apply(null);

        //Then
        assertThat(result.getEstimate()).isEqualTo(0);
    }

    @Test
    public void shouldCreateHllSketch() {
        //Given
        IterableToHllSketch iterableToHllSketch = new IterableToHllSketch();
        List<Object> input = Arrays.asList("one", "two", "three", "four", "five");

        //When
        HllSketch result = iterableToHllSketch.apply(input);

        //Then
        assertThat(result.getEstimate()).isCloseTo(5, Percentage.withPercentage(0.001));
    }

    @Test
    public void shouldCreateHllSketchCardinality() {
        //Given
        IterableToHllSketch iterableToHllSketch = new IterableToHllSketch();
        List<Object> input = Arrays.asList("one", "one", "two", "two", "three");

        //When
        HllSketch result = iterableToHllSketch.apply(input);

        //Then
        assertThat(result.getEstimate()).isCloseTo(3, Percentage.withPercentage(0.001));
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
        IterableToHllSketch deserialisedIterableToHllSketch = JSONSerialiser.deserialise(json, IterableToHllSketch.class);
        // Then
        assertEquals(iterableToHllSketch, deserialisedIterableToHllSketch);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.sketches.datasketches.cardinality.function.IterableToHllSketch\"}", json);
    }

    @Override
    protected IterableToHllSketch getInstance() {
        return new IterableToHllSketch();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}
