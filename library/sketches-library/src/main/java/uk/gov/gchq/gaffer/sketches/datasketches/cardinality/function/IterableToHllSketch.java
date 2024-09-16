/*
 * Copyright 2019-2023 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.datasketches.hll.HllSketch;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.sketches.datasketches.cardinality.serialisation.json.HllSketchJsonConstants.DEFAULT_LOG_K;

/**
 * Creates a new {@link HllSketch} instance and initialises it from
 * the given iterable.
 */
@Since("1.21.0")
@JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
@Summary("Creates a new HllSketch instance and initialises it from the given iterable")
public class IterableToHllSketch extends KorypheFunction<Iterable<Object>, HllSketch> {
    private int logK = DEFAULT_LOG_K;
    @JsonIgnore
    private HllSketch hllSketch;

    public IterableToHllSketch() {
        hllSketch = new HllSketch(logK);
    }

    public IterableToHllSketch(final int logK) {
        this.logK = logK;
        hllSketch = new HllSketch(logK);
    }

    public IterableToHllSketch(final HllSketch hllSketch) {
        setHllSketch(hllSketch);
    }

    @Override
    public HllSketch apply(final Iterable<Object> iterable) {
        if (nonNull(iterable)) {
            for (final Object o : iterable) {
                if (nonNull(o)) {
                    if (o instanceof String) {
                        hllSketch.update((String) o);
                    } else if (o instanceof Long) {
                        hllSketch.update(((long) o));
                    } else if (o instanceof Integer) {
                        hllSketch.update(((int) o));
                    } else if (o instanceof byte[]) {
                        hllSketch.update(((byte[]) o));
                    } else if (o instanceof Double) {
                        hllSketch.update(((double) o));
                    } else if (o instanceof char[]) {
                        hllSketch.update(((char[]) o));
                    } else if (o instanceof long[]) {
                        hllSketch.update(((long[]) o));
                    } else if (o instanceof int[]) {
                        hllSketch.update(((int[]) o));
                    } else {
                        hllSketch.update(o.toString());
                    }
                }
            }
        }
        return hllSketch;
    }

    public int getLogK() {
        return logK;
    }

    public void setLogK(final int logK) {
        this.logK = logK;
    }

    public HllSketch getHllSketch() {
        return hllSketch;
    }

    public void setHllSketch(final HllSketch hllSketch) {
        if (hllSketch != null) {
            this.hllSketch = hllSketch.copy();
        } else {
            this.hllSketch = new HllSketch(logK);
        }
    }
}
