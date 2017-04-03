/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.bitmap.function.aggregate;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.roaringbitmap.RoaringBitmap;
import uk.gov.gchq.gaffer.function.AggregateFunction;
import uk.gov.gchq.gaffer.function.AggregateFunctionTest;
import uk.gov.gchq.gaffer.function.Function;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class RoaringBitmapAggregatorTest extends AggregateFunctionTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void aggregatorDealsWithNullInput() {
        RoaringBitmapAggregator roaringBitmapAggregator = new RoaringBitmapAggregator();
        roaringBitmapAggregator.init();
        roaringBitmapAggregator._aggregate(null);
        assertNull(roaringBitmapAggregator.state()[0]);
    }

    @Test
    public void emptyInputBitmapGeneratesEmptyOutputBitmap() {
        RoaringBitmap bitmap = new RoaringBitmap();
        RoaringBitmapAggregator roaringBitmapAggregator = new RoaringBitmapAggregator();
        roaringBitmapAggregator.init();
        roaringBitmapAggregator._aggregate(bitmap);

        assertEquals(0, ((RoaringBitmap) roaringBitmapAggregator.state()[0]).getCardinality());
    }

    @Test
    public void singleInputBitmapGeneratesIdenticalOutputBitmap() {
        RoaringBitmap inputBitmap = new RoaringBitmap();
        int input1 = 123298333;
        int input2 = 342903339;
        inputBitmap.add(input1);
        inputBitmap.add(input2);

        RoaringBitmapAggregator roaringBitmapAggregator = new RoaringBitmapAggregator();
        roaringBitmapAggregator.init();
        roaringBitmapAggregator._aggregate(inputBitmap);

        assertEquals(2, ((RoaringBitmap) roaringBitmapAggregator.state()[0]).getCardinality());
        assertEquals(inputBitmap, roaringBitmapAggregator.state()[0]);
    }

    @Test
    public void threeOverlappingInputBitmapsProducesSingleSortedBitmap() {
        int[] inputs = new int[6];
        RoaringBitmap inputBitmap1 = new RoaringBitmap();
        int input1 = 23615000;
        int input2 = 23616440;
        inputBitmap1.add(input1);
        inputBitmap1.add(input2);
        inputs[0] = input1;
        inputs[1] = input2;

        RoaringBitmapAggregator roaringBitmapAggregator = new RoaringBitmapAggregator();
        roaringBitmapAggregator._aggregate(inputBitmap1);
        assertEquals(inputBitmap1, roaringBitmapAggregator.state()[0]);

        RoaringBitmap inputBitmap2 = new RoaringBitmap();
        int input3 = 23615003;
        int input4 = 23615018;
        inputBitmap2.add(input3);
        inputBitmap2.add(input4);
        inputs[2] = input3;
        inputs[3] = input4;
        roaringBitmapAggregator._aggregate(inputBitmap2);

        RoaringBitmap inputBitmap3 = new RoaringBitmap();
        int input5 = 23615002;
        int input6 = 23615036;
        inputBitmap3.add(input5);
        inputBitmap3.add(input6);
        inputs[4] = input5;
        inputs[5] = input6;
        roaringBitmapAggregator._aggregate(inputBitmap3);

        Arrays.sort(inputs);
        RoaringBitmap result = (RoaringBitmap) roaringBitmapAggregator.state()[0];
        int outPutBitmapSize = result.getCardinality();
        assertEquals(6, outPutBitmapSize);
        int i = 0;
        for (Integer value : result) {
            assertEquals((Integer) inputs[i], value);
            i++;
        }
    }

    @Test
    public void shouldCloneInputBitmapWhenAggregating() {
        // Given
        final RoaringBitmapAggregator roaringBitmapAggregator = new RoaringBitmapAggregator();
        roaringBitmapAggregator.init();

        final RoaringBitmap bitmap = mock(RoaringBitmap.class);
        final RoaringBitmap clonedBitmap = mock(RoaringBitmap.class);
        given(bitmap.clone()).willReturn(clonedBitmap);

        // When
        roaringBitmapAggregator._aggregate(bitmap);

        // Then
        assertSame(clonedBitmap, roaringBitmapAggregator.state()[0]);
        assertNotSame(bitmap, roaringBitmapAggregator.state()[0]);
    }

    @Override
    protected AggregateFunction getInstance() {
        return new RoaringBitmapAggregator();
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return RoaringBitmapAggregator.class;
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        final RoaringBitmapAggregator roaringBitmapAggregator = new RoaringBitmapAggregator();

        String serialisedForm = this.serialise(roaringBitmapAggregator);

        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.bitmap.function.aggregate.RoaringBitmapAggregator\"}", serialisedForm);
        assertEquals(roaringBitmapAggregator, this.deserialise(serialisedForm));
    }
}
