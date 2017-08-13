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
import org.apache.spark.sql.SparkSession;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.Converter;
import uk.gov.gchq.gaffer.spark.serialisation.TypeReferenceSparkImpl;
import java.util.List;
import java.util.Map;

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
public class GetDataFrameOfElements implements
        Operation,
        Output<Dataset<Row>>,
        GraphFilters,
        Options {

    @Required
    private SparkSession sparkSession;
    private List<Converter> converters;
    private Map<String, String> options;
    private View view;
    private DirectedType directedType;

    public GetDataFrameOfElements() {
    }

    public GetDataFrameOfElements(final SparkSession sparkSession,
                                  final List<Converter> converters) {
        this();
        this.sparkSession = sparkSession;
        this.converters = converters;
    }

    public void setSparkSession(final SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setConverters(final List<Converter> converters) {
        this.converters = converters;
    }

    public List<Converter> getConverters() {
        return converters;
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
    public TypeReference<Dataset<Row>> getOutputTypeReference() {
        return new TypeReferenceSparkImpl.DataSetRow();
    }

    @Override
    public View getView() {
        return view;
    }

    @Override
    public void setView(final View view) {
        this.view = view;
    }

    @Override
    public DirectedType getDirectedType() {
        return directedType;
    }

    @Override
    public void setDirectedType(final DirectedType directedType) {
        this.directedType = directedType;
    }

    public static class Builder extends Operation.BaseBuilder<GetDataFrameOfElements, Builder>
            implements Output.Builder<GetDataFrameOfElements, Dataset<Row>, Builder>,
            Options.Builder<GetDataFrameOfElements, Builder>,
            GraphFilters.Builder<GetDataFrameOfElements, Builder> {
        public Builder() {
            super(new GetDataFrameOfElements());
        }

        public Builder sparkSession(final SparkSession sparkSession) {
            _getOp().setSparkSession(sparkSession);
            return _self();
        }

        public Builder converters(final List<Converter> converters) {
            _getOp().setConverters(converters);
            return _self();
        }
    }
}
