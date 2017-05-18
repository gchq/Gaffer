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

package uk.gov.gchq.gaffer.operation;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Closeable;
import java.io.IOException;

/**
 * An <code>Operation</code> defines an operation to be processed on a graph.
 * All operations must to implement this interface.
 * Operations should be written to be as generic as possible to allow them to be applied to different graph/stores.
 * NOTE - operations should not contain the operation logic. The logic should be separated out into a operation handler.
 * This will allow you to execute the same operation on different stores with different handlers.
 * <p>
 * Operations must be JSON serialisable in order to make REST API calls.
 * </p>
 * <p>
 * Operation implementations need to implement this Operation interface and any of the following interfaces they wish to make use of:
 * {@link uk.gov.gchq.gaffer.operation.io.Input}
 * {@link uk.gov.gchq.gaffer.operation.io.Output}
 * {@link uk.gov.gchq.gaffer.operation.io.InputOutput} (Use this instead of Input and Output if your operation takes both input and output.)
 * {@link uk.gov.gchq.gaffer.operation.io.MultiInput} (Use this in addition if you operation takes multiple inputs. This will help with json  serialisation)
 * {@link uk.gov.gchq.gaffer.operation.SeedMatching}
 * {@link uk.gov.gchq.gaffer.operation.Validatable}
 * {@link uk.gov.gchq.gaffer.operation.graph.OperationView}
 * {@link uk.gov.gchq.gaffer.operation.graph.GraphFilters}
 * {@link uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters}
 * {@link uk.gov.gchq.gaffer.operation.Options}
 * </p>
 * <p>
 * Each Operation implementation should have a corresponding unit test class
 * that extends the OperationTest class.
 * </p>
 * <p>
 * Implementations should override the close method and ensure all closeable fields are closed.
 * </p>
 * <p>
 * All implementations should also have a static inner Builder class that implements
 * the required builders. For example:
 * </p>
 * <pre>
 * public static class Builder extends Operation.BaseBuilder&lt;GetElements, Builder&gt;
 *         implements InputOutput.Builder&lt;GetElements, Iterable&lt;? extends ElementId&gt;, CloseableIterable&lt;? extends Element&gt;, Builder&gt;,
 *         MultiInput.Builder&lt;GetElements, ElementId, Builder&gt;,
 *         SeededGraphFilters.Builder&lt;GetElements, Builder&gt;,
 *         SeedMatching.Builder&lt;GetElements, Builder&gt;,
 *         Options.Builder&lt;GetElements, Builder&gt; {
 *     public Builder() {
 *             super(new GetElements());
 *     }
 * }
 * </pre>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public interface Operation extends Closeable {

    /**
     * Operation implementations should ensure that all closeable fields are closed in this method.
     *
     * @throws IOException if an I/O error occurs
     */
    default void close() throws IOException {
        // do nothing by default
    }

    interface Builder<OP, B extends Builder<OP, ?>> {
        OP _getOp();

        B _self();
    }

    abstract class BaseBuilder<OP extends Operation, B extends BaseBuilder<OP, ?>>
            implements Builder<OP, B> {
        private OP op;

        protected BaseBuilder(final OP op) {
            this.op = op;
        }

        /**
         * Builds the operation and returns it.
         *
         * @return the built operation.
         */
        public OP build() {
            return _getOp();
        }

        @Override
        public OP _getOp() {
            return op;
        }

        @Override
        public B _self() {
            return (B) this;
        }
    }
}

