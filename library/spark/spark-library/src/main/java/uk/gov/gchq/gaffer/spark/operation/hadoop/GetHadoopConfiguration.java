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

package uk.gov.gchq.gaffer.spark.operation.hadoop;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.hadoop.conf.Configuration;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.spark.TypeReferenceHadoopConfigurationImpl;

import java.util.Map;

public class GetHadoopConfiguration implements Output<Configuration>, GraphFilters {

    private Map<String, String> options;
    private View view;
    private DirectedType directedType;

    public GetHadoopConfiguration() {
        this.view = new View.Builder()
                .build();
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
    public TypeReference<Configuration> getOutputTypeReference() {
        return new TypeReferenceHadoopConfigurationImpl.HadoopConfiguration();
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
    public GetHadoopConfiguration shallowClone() {
        return new Builder()
                .options(options)
                .view(view)
                .directedType(directedType)
                .build();
    }

    public static class Builder extends Operation.BaseBuilder<GetHadoopConfiguration, Builder>
            implements Output.Builder<GetHadoopConfiguration, Configuration, Builder>,
            GraphFilters.Builder<GetHadoopConfiguration, Builder>,
            Operation.Builder<GetHadoopConfiguration, Builder> {
        public Builder() {
            super(new GetHadoopConfiguration());
        }
    }




}

