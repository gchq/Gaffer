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
package uk.gov.gchq.gaffer.spark.operation.javardd;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.spark.api.java.JavaRDD;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.spark.serialisation.TypeReferenceSparkImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code GetJavaRDDOfAllElements} operation retrieves all the {@link Element}s
 * from the target store, and returns them inside a {@link JavaRDD}.
 */
@JsonPropertyOrder(value = {"class", "view"}, alphabetic = true)
@Since("1.0.0")
@Summary("Gets a JavaRDD of all elements")
public class GetJavaRDDOfAllElements implements
        Output<JavaRDD<Element>>,
        GraphFilters {

    private Map<String, String> options;
    private View view;
    private DirectedType directedType;

    public GetJavaRDDOfAllElements() {
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
    public TypeReference<JavaRDD<Element>> getOutputTypeReference() {
        return new TypeReferenceSparkImpl.JavaRDDElement();
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
    public GetJavaRDDOfAllElements shallowClone() {
        return new GetJavaRDDOfAllElements.Builder()
                .options(options)
                .view(view)
                .directedType(directedType)
                .build();
    }

    public static class Builder extends BaseBuilder<GetJavaRDDOfAllElements, Builder>
            implements Output.Builder<GetJavaRDDOfAllElements, JavaRDD<Element>, Builder>,
            GraphFilters.Builder<GetJavaRDDOfAllElements, Builder>,
            Operation.Builder<GetJavaRDDOfAllElements, Builder> {
        public Builder() {
            super(new GetJavaRDDOfAllElements());
        }
    }
}
