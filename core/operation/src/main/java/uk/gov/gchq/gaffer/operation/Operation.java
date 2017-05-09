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

/**
 * An <code>Operation</code> defines an operation to be processed on a graph.
 * All operations must to implement this class.
 * Operations should be written to be as generic as possible to allow them to be applied to different graph/stores.
 * NOTE - operations should not contain the operation logic. The logic should be separated out into a operation handler.
 * This will allow you to execute the same operation on different stores with different handlers.
 * <p>
 * Operations must be JSON serialisable in order to make REST API calls.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public interface Operation {
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

