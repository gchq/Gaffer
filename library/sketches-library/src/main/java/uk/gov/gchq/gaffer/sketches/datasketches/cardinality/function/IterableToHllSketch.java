/*
 * Copyright 2019 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.yahoo.sketches.hll.HllSketch;

import uk.gov.gchq.koryphe.function.KorypheFunction;

import static java.util.Objects.nonNull;

public class IterableToHllSketch extends KorypheFunction<Iterable<Object>, HllSketch> {
    @JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
    private int logK = 5;

    public IterableToHllSketch() {
    }

    public IterableToHllSketch(final int logK) {
        this.logK = logK;
    }

    @Override
    public HllSketch apply(final Iterable<Object> iterable) {
        final HllSketch hllp = new HllSketch(logK);
        for (final Object o : iterable) {
            if (nonNull(o)) {
                if (o instanceof String) {
                    hllp.update((String) o);
                } else if (o instanceof Long) {
                    hllp.update(((long) o));
                } else if (o instanceof byte[]) {
                    hllp.update(((byte[]) o));
                } else if (o instanceof Double) {
                    hllp.update(((double) o));
                } else if (o instanceof char[]) {
                    hllp.update(((char[]) o));
                } else if (o instanceof long[]) {
                    hllp.update(((long[]) o));
                } else if (o instanceof int[]) {
                    hllp.update(((int[]) o));
                } else {
                    hllp.update(o.toString());
                }
            }
        }
        return hllp;
    }

    public int getLogK() {
        return logK;
    }

    public void setLogK(final int logK) {
        this.logK = logK;
    }
}

