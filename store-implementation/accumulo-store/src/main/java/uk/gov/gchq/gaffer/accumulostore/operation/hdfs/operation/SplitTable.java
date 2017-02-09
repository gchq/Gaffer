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
package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation;

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.operation.AbstractOperation;
import uk.gov.gchq.gaffer.operation.VoidOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;


/**
 * The <code>SplitTable</code> operation is for splitting an accumulo table based on a sequence file of split points.
 *
 * @see SplitTable.Builder
 */
public class SplitTable extends AbstractOperation<String, Void> implements VoidOutput<String> {
    private String inputPath;

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(final String inputPath) {
        this.inputPath = inputPath;
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.Void();
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractOperation.BaseBuilder<SplitTable, String, Void, CHILD_CLASS> {
        public BaseBuilder() {
            super(new SplitTable());
        }

        public CHILD_CLASS inputPath(final String inputPath) {
            op.setInputPath(inputPath);
            return self();
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
