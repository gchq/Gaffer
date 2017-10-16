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
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.Converter;
import uk.gov.gchq.gaffer.spark.serialisation.TypeReferenceSparkImpl;

import java.util.List;
import java.util.Map;

/**
 * An {@code Operation} that returns an Apache Spark {@code GraphFrame} (i.e. an abstraction over a
 * {@link org.apache.spark.sql.Dataset} of {@link org.apache.spark.sql.Row}s) consisting of the
 * {@code Elements}s converted to rows.
 *
 * This GraphFrame object can be used as the basis for a number of graph processing queries.
 *
 * @see uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements
 */
public class GetGraphFrameOfElements implements
        Output<GraphFrame>,
        GraphFilters {

    @Required
    private SparkSession sparkSession;
    private List<Converter> converters;
    private Map<String, String> options;
    private View view;
    private DirectedType directedType;

    public GetGraphFrameOfElements() {
    }

    public GetGraphFrameOfElements(final SparkSession sparkSession,
                                  final List<Converter> converters) {
        this();
        this.sparkSession = sparkSession;
        this.converters = converters;
    }

    @Override
    public TypeReference<GraphFrame> getOutputTypeReference() {
        return new TypeReferenceSparkImpl.GraphFrame();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(final SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public List<Converter> getConverters() {
        return converters;
    }

    public void setConverters(final List<Converter> converters) {
        this.converters = converters;
    }

    public View getView() {
        return view;
    }

    public void setView(final View view) {
        this.view = view;
    }

    public DirectedType getDirectedType() {
        return directedType;
    }

    public void setDirectedType(final DirectedType directedType) {
        this.directedType = directedType;
    }

    @Override
    public GetGraphFrameOfElements shallowClone() {
        return new GetGraphFrameOfElements.Builder()
                .sparkSession(sparkSession)
                .converters(converters)
                .options(options)
                .directedType(directedType)
                .view(view)
                .build();
    }

    public static class Builder extends Operation.BaseBuilder<GetGraphFrameOfElements, Builder>
            implements Output.Builder<GetGraphFrameOfElements, GraphFrame, Builder>,
            GraphFilters.Builder<GetGraphFrameOfElements, Builder> {
        public Builder() {
            super(new GetGraphFrameOfElements());
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
