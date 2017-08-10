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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.spark.serialisation.TypeReferenceSparkImpl;
import java.util.Map;

public class GetRDDOfElements implements
        Operation,
        InputOutput<Iterable<? extends ElementId>, RDD<Element>>,
        MultiInput<ElementId>,
        SeededGraphFilters,
        Rdd,
        Options {

    private Map<String, String> options;
    @Required
    private SparkSession sparkSession;
    private Iterable<? extends ElementId> input;
    private IncludeIncomingOutgoingType inOutType;
    private View view;
    private DirectedType directedType;

    public GetRDDOfElements() {
    }

    public GetRDDOfElements(final SparkSession sparkSession) {
        setSparkSession(sparkSession);
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
    public TypeReference<RDD<Element>> getOutputTypeReference() {
        return new TypeReferenceSparkImpl.RDDElement();
    }

    @Override
    public SparkSession getSparkSession() {
        return sparkSession;
    }

    @Override
    public void setSparkSession(final SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public Iterable<? extends ElementId> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends ElementId> input) {
        this.input = input;
    }

    @Override
    public IncludeIncomingOutgoingType getIncludeIncomingOutGoing() {
        return inOutType;
    }

    @Override
    public void setIncludeIncomingOutGoing(final IncludeIncomingOutgoingType inOutType) {
        this.inOutType = inOutType;
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

    public static class Builder extends Operation.BaseBuilder<GetRDDOfElements, Builder>
            implements InputOutput.Builder<GetRDDOfElements, Iterable<? extends ElementId>, RDD<Element>, Builder>,
            MultiInput.Builder<GetRDDOfElements, ElementId, Builder>,
            SeededGraphFilters.Builder<GetRDDOfElements, Builder>,
            Rdd.Builder<GetRDDOfElements, Builder>,
            Options.Builder<GetRDDOfElements, Builder> {
        public Builder() {
            super(new GetRDDOfElements());
        }
    }
}
