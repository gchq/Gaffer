/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.spark.operation.dataframe;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import uk.gov.gchq.gaffer.operation.AbstractGetOperation;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.Converter;
import uk.gov.gchq.gaffer.spark.serialisation.TypeReferenceSparkImpl;
import java.util.List;

/**
 * An <code>Operation</code> that returns an Apache Spark <code>DataFrame</code> (i.e. a {@link Dataset} of
 * {@link Row}s) consisting of the <code>Element</code>s converted to {@link Row}s. The fields in the {@link Row}
 * are ordered according to the ordering of the groups in the view, with <code>Entity</code>s first,
 * followed by <code>Edge</code>s.
 * <p>
 * Implementations of this operation should automatically convert all properties that have natural equivalents
 * as a Spark <code>DataType</code> to that <code>DataType</code>. An implementation may allow the user to
 * specify a conversion function for properties that do not have natural equivalents. Thus not all properties
 * from each <code>Element</code> will necessarily make it into the <code>DataFrame</code>.
 * <p>
 * The schema of the <code>Dataframe</code> is formed of all properties from the first group, followed by all
 * properties from the second group, with the exception of properties already found in the first group, etc.
 */
public class GetDataFrameOfElements extends AbstractGetOperation<Void, Dataset<Row>> {

    private SQLContext sqlContext;
    private List<Converter> converters;

    public GetDataFrameOfElements() {
    }

    public GetDataFrameOfElements(final SQLContext sqlContext,
                                  final List<Converter> converters) {
        this();
        this.sqlContext = sqlContext;
        this.converters = converters;
    }

    public void setSqlContext(final SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    public SQLContext getSqlContext() {
        return sqlContext;
    }

    public void setConverters(final List<Converter> converters) {
        this.converters = converters;
    }

    public List<Converter> getConverters() {
        return converters;
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceSparkImpl.DataSetRow();
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractGetOperation.BaseBuilder<GetDataFrameOfElements, Void, Dataset<Row>, CHILD_CLASS> {

        public BaseBuilder() {
            this(new GetDataFrameOfElements());
        }

        public BaseBuilder(final GetDataFrameOfElements op) {
            super(op);
        }

        public CHILD_CLASS sqlContext(final SQLContext sqlContext) {
            op.setSqlContext(sqlContext);
            return self();
        }

        public CHILD_CLASS converters(final List<Converter> converters) {
            op.setConverters(converters);
            return self();
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        public Builder() {
        }

        public Builder(final GetDataFrameOfElements op) {
            super(op);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
