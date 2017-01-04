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
package uk.gov.gchq.gaffer.function.aggregate;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.SimpleAggregateFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;
import java.util.ArrayList;

/**
 * An <code>ArrayListConcat</code> is a {@link SimpleAggregateFunction} that concatenates
 * {@link ArrayList}s together.
 */
@Inputs(ArrayList.class)
@Outputs(ArrayList.class)
public class ArrayListConcat extends SimpleAggregateFunction<ArrayList<Object>> {
    private ArrayList<Object> result;

    @Override
    protected void _aggregate(final ArrayList<Object> input) {
        if (null != input) {
            if (null == result) {
                result = new ArrayList<>(input);
            } else {
                result.addAll(input);
            }
        }
    }

    @Override
    public void init() {
        result = null;
    }

    @Override
    protected ArrayList<Object> _state() {
        return result;
    }

    public ArrayListConcat statelessClone() {
        final ArrayListConcat arrayListConcat = new ArrayListConcat();
        arrayListConcat.init();

        return arrayListConcat;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ArrayListConcat that = (ArrayListConcat) o;

        return new EqualsBuilder()
                .append(inputs, that.inputs)
                .append(outputs, that.outputs)
                .append(result, that.result)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(inputs)
                .append(outputs)
                .append(result)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("outputs", outputs)
                .append("result", result)
                .toString();
    }
}
