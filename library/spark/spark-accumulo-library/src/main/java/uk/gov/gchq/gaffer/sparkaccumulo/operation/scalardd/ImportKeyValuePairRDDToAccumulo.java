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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.scalardd;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import uk.gov.gchq.gaffer.operation.AbstractOperation;
import uk.gov.gchq.gaffer.operation.VoidOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

public class ImportKeyValuePairRDDToAccumulo extends AbstractOperation<RDD<Tuple2<Key, Value>>, Void> implements VoidOutput<RDD<Tuple2<Key, Value>>> {

    private String outputPath;

    private String failurePath;

    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.Void();
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(final String outputPath) {
        this.outputPath = outputPath;
    }

    public void setFailurePath(final String failurePath) {
        this.failurePath = failurePath;
    }

    public String getFailurePath() {
        return failurePath;
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractOperation.BaseBuilder<ImportKeyValuePairRDDToAccumulo, RDD<Tuple2<Key, Value>>, Void, CHILD_CLASS> {
        public BaseBuilder() {
            super(new ImportKeyValuePairRDDToAccumulo());
        }

        public CHILD_CLASS outputPath(final String outputPath) {
            op.setOutputPath(outputPath);
            return self();
        }

        public CHILD_CLASS failurePath(final String failurePath) {
            op.setFailurePath(failurePath);
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
