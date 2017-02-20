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
package uk.gov.gchq.gaffer.spark.operation.scalardd;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.AbstractOperation;
import uk.gov.gchq.gaffer.operation.VoidOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

public class ImportRDDOfElements extends AbstractOperation<RDD<Element>, Void> implements VoidOutput<RDD<Element>> {
    private SparkContext sparkContext;
    public static final String HADOOP_CONFIGURATION_KEY = "Hadoop_Configuration_Key";

    public SparkContext getSparkContext() {
        return sparkContext;
    }

    public void setSparkContext(final SparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.Void();
    }

    protected abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractOperation.BaseBuilder<ImportRDDOfElements, RDD<Element>, Void, CHILD_CLASS> {
        public BaseBuilder() {
            super(new ImportRDDOfElements());
        }

        public CHILD_CLASS sparkContext(final SparkContext sparkContext) {
            op.setSparkContext(sparkContext);
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
