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
package uk.gov.gchq.gaffer.spark.operation.scalardd;

import org.apache.spark.SparkContext;

public class GetRDDOfAllElements extends AbstractGetRDD<Void> {

    public GetRDDOfAllElements() {
    }

    public GetRDDOfAllElements(final SparkContext sparkContext) {
        setSparkContext(sparkContext);
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractGetRDD.BaseBuilder<GetRDDOfAllElements, Void, CHILD_CLASS> {

        public BaseBuilder() {
            this(new GetRDDOfAllElements());
        }

        public BaseBuilder(final GetRDDOfAllElements op) {
            super(op);
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        public Builder() {
        }

        public Builder(final GetRDDOfAllElements op) {
            super(op);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
