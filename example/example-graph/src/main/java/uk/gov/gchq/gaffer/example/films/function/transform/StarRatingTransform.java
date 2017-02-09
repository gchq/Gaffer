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

import uk.gov.gchq.gaffer.function.TransformFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;

@Inputs({Long.class, Integer.class})
@Outputs(Float.class)
public class StarRatingTransform extends TransformFunction {
    @Override
    public Object[] transform(final Object[] input) {
        return execute((Long) input[0], (Integer) input[1]);
    }

    public Object[] execute(final Long total, final Integer count) {
        final float average = total / (float) count;

        // Convert the average to a star rating between 0 and 5 - rounded to 2 decimal places
        final float fiveStarRating = Math.round(5 * average) / 100f;

        return new Object[]{fiveStarRating};
    }

    public StarRatingTransform statelessClone() {
        return new StarRatingTransform();
    }
}
