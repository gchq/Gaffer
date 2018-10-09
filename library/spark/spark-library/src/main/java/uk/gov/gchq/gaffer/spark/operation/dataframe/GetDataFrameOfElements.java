/*
 * Copyright 2016-2018 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.Converter;
import uk.gov.gchq.gaffer.spark.serialisation.TypeReferenceSparkImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.List;
import java.util.Map;

/**
 * An {@code Operation} that returns an Apache Spark {@code DataFrame} (i.e. a {@link Dataset} of
 * {@link Row}s) consisting of the {@code Element}s converted to {@link Row}s. The fields in the {@link Row}
 * are ordered according to the ordering of the groups in the view, with {@code Entity}s first,
 * followed by {@code Edge}s.
 * <p>
 * Implementations of this operation should automatically convert all properties that have natural equivalents
 * as a Spark {@code DataType} to that {@code DataType}. An implementation may allow the user to
 * specify a conversion function for properties that do not have natural equivalents. Thus not all properties
 * from each {@code Element} will necessarily make it into the {@code DataFrame}.
 * <p>
 * The schema of the {@code Dataframe} is formed of all properties from the first group, followed by all
 * properties from the second group, with the exception of properties already found in the first group, etc.
 */
@JsonPropertyOrder(value = {"class", "view"}, alphabetic = true)
@Since("1.0.0")
@Summary("Gets a DataFrame of elements")
public class GetDataFrameOfElements implements
        Output<Dataset<Row>>,
        GraphFilters {

    private List<Converter> converters;
    private Map<String, String> options;
    private View view;
    private DirectedType directedType;

    public GetDataFrameOfElements() {
    }

    public GetDataFrameOfElements(final List<Converter> converters) {
        this.converters = converters;
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

    @Override
    public GetDataFrameOfElements shallowClone() {
        return new GetDataFrameOfElements.Builder()
                .converters(converters)
                .options(options)
                .directedType(directedType)
                .view(view)
                .build();
    }

    public static class Builder extends Operation.BaseBuilder<GetDataFrameOfElements, Builder>
            implements Output.Builder<GetDataFrameOfElements, Dataset<Row>, Builder>,
            GraphFilters.Builder<GetDataFrameOfElements, Builder> {
        public Builder() {
            super(new GetDataFrameOfElements());
        }

        public Builder converters(final List<Converter> converters) {
            _getOp().setConverters(converters);
            return _self();
        }
    }
}
