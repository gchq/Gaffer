/*
 * Copyright 2023 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.serialisation.json;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.datasketches.hll.HllSketch;

import uk.gov.gchq.gaffer.sketches.datasketches.cardinality.function.IterableToHllSketch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static uk.gov.gchq.gaffer.sketches.datasketches.cardinality.serialisation.json.HllSketchJsonConstants.DEFAULT_LOG_K;

/**
 * A {@code HllSketchWithValues} is a wrapper around the {@link HllSketch} object
 * which helps Gaffer deserialise them.
 *
 * The {@code values} list is taken and fed into an {@link IterableToHllSketch}
 * function so that they are applied over JSON. This effectively allows you to create
 * a populated {@link HllSketch} over JSON.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HllSketchWithValues {

    private int logK = DEFAULT_LOG_K;

    private byte[] bytes = null;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
    private List<Object> values = new ArrayList<>();

    public int getLogK() {
        return logK;
    }

    public void setLogK(final int logK) {
        this.logK = logK;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(final byte[] bytes) {
        this.bytes = bytes;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(final List<Object> values) {
        this.values = values;
    }

    @JsonIgnore
    public HllSketch getHllSketch() throws IOException {
        final IterableToHllSketch toHllSketch;
        if (getBytes() == null) {
            toHllSketch = new IterableToHllSketch(getLogK());
        } else {
            toHllSketch = new IterableToHllSketch(HllSketch.heapify(getBytes()));
        }
        return toHllSketch.apply(getValues());
    }
}
