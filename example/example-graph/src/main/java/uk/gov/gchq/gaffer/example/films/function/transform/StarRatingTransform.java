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

package uk.gov.gchq.gaffer.example.films.function.transform;

import uk.gov.gchq.koryphe.tuple.function.KorypheFunction2;

public class StarRatingTransform extends KorypheFunction2<Long, Integer, Float> {
    @Override
    public Float apply(final Long total, final Integer count) {
        final float average = total / (float) count;

        // Convert the average to a star rating between 0 and 5 - rounded to 2 decimal places
        return Math.round(5 * average) / 100f;
    }
}
