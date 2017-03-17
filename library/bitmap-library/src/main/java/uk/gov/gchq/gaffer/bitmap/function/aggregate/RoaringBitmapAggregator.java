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

import org.roaringbitmap.RoaringBitmap;
import uk.gov.gchq.gaffer.function.AggregateFunction;
import uk.gov.gchq.gaffer.function.SimpleAggregateFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;

@Inputs(RoaringBitmap.class)
@Outputs(RoaringBitmap.class)
public class RoaringBitmapAggregator extends SimpleAggregateFunction<RoaringBitmap> {
    private RoaringBitmap result = null;

    @Override
    protected void _aggregate(final RoaringBitmap input) {
        if (null == input) {
            return;
        }
        if (null == result) {
            result = input.clone();
            return;
        }
        result.or(input);
    }

    @Override
    protected RoaringBitmap _state() {
        return result;
    }

    @Override
    public void init() {

    }

    @Override
    public AggregateFunction statelessClone() {
        return new RoaringBitmapAggregator();
    }
}
