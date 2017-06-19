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

import uk.gov.gchq.gaffer.bitmap.types.MapOfBitmaps;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

public class MapOfBitmapsAggregator extends KorypheBinaryOperator<MapOfBitmaps> {

    private static final RoaringBitmapAggregator BITMAP_AGGREGATOR = new RoaringBitmapAggregator();

    @Override
    protected MapOfBitmaps _apply(final MapOfBitmaps a, final MapOfBitmaps b) {
        b.forEach((k, v) -> a.merge(k, v, BITMAP_AGGREGATOR));
        return a;
    }
}
