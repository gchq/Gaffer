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

package uk.gov.gchq.gaffer.spark.operation.graphframe;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;

import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.spark.serialisation.TypeReferenceSparkImpl;

import java.util.Map;

/**
 * An {@code Operation} that converts an Apache Spark {@code GraphFrame} (i.e. an
 * abstraction over a {@link org.apache.spark.sql.Dataset} of {@link
 * org.apache.spark.sql.Row}s) to an {@link Iterable} of {@link Row} objects.
 * <p>
 */
public class GraphFrameAsIterable implements
        InputOutput<GraphFrame, Iterable<? extends Row>> {

    private GraphFrame input;
    private Map<String, String> options;

    public GraphFrameAsIterable() {
    }

    @Override
    public TypeReference<Iterable<? extends Row>> getOutputTypeReference() {
        return new TypeReferenceSparkImpl.IterableRowT();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public GraphFrameAsIterable shallowClone() {
        return new GraphFrameAsIterable.Builder()
                .options(options)
                .input(input)
                .build();
    }

    @Override
    public GraphFrame getInput() {
        return input;
    }

    @Override
    public void setInput(final GraphFrame input) {
        this.input = input;
    }

    public static class Builder extends BaseBuilder<GraphFrameAsIterable, Builder>
            implements InputOutput.Builder<GraphFrameAsIterable, GraphFrame, Iterable<? extends Row>, Builder> {
        public Builder() {
            super(new GraphFrameAsIterable());
        }
    }
}
