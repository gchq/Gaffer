/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.subOperation;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.QueryOperationChain1;
import uk.gov.gchq.gaffer.operation.graph.OperationView;

import java.util.Objects;

public class ElementDirection2<OUT> extends QueryOperationChain1<OUT> implements ElementDirection {

    private Elements element;
    private View.Builder viewBuilder;
    private String group;

    private ViewElementDefinition.Builder viewElementDefinitionBuilder;

    public ElementDirection2(final OperationChain<OUT> chain) {
        super(chain);
        if (chain instanceof ElementDirection2) {
            ElementDirection2 that = (ElementDirection2) chain;
            this.element = that.element;
            this.viewBuilder = that.viewBuilder;
            this.group = that.group;
            this.viewElementDefinitionBuilder = that.viewElementDefinitionBuilder;
        }
    }

    protected void tidyUp() {
        localTidyUp();
        final BaseBuilder currentOperationBuilder = getCurrentOperationBuilder();
        if (Objects.nonNull(viewBuilder)) {
            if (currentOperationBuilder instanceof OperationView.Builder) {
                ((OperationView.Builder) currentOperationBuilder).view(viewBuilder.build());
            }
        }

        clear();
        super.tidyUp();
    }

    private void localTidyUp() {
        if (Objects.nonNull(element)) {
            switch (element) {
                case edge: {
                    ViewElementDefinition viewElementDefinition = null;
                    if (Objects.nonNull(viewElementDefinitionBuilder)) {
                        viewElementDefinition = viewElementDefinitionBuilder.build();
                    }

                    if (Objects.isNull(viewBuilder)) {
                        viewBuilder = new View.Builder();
                    }

                    viewBuilder.edge(group, viewElementDefinition);

                    localClear();

                    break;
                }
                default:
                    throw new UnsupportedOperationException("Not yet implemented");
            }
            localClear();
        }
    }

    private void localClear() {
        this.element = null;
        this.group = null;
        this.viewElementDefinitionBuilder = null;
    }

    private void clear() {
        this.element = null;
        this.viewBuilder = null;
        this.group = null;
        this.viewElementDefinitionBuilder = null;
    }

    @Override
    public ViewElementLevel3 edge2(final String group) {
        if (Objects.nonNull(this.group)) {
            localTidyUp();
        }
        this.element = Elements.edge;
        this.group = group;
        return new ViewElementLevel3(this);
    }


    @Override
    public ViewElementLevel3 entity2(final String group1) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }


    enum Elements {
        edge, entity;

    }

    protected ViewElementDefinition.Builder getViewElementDefinitionBuilder() {
        return viewElementDefinitionBuilder;
    }

    public ElementDirection2 viewElementDefinitionBuilder(final ViewElementDefinition.Builder viewElementDefinitionBuilder) {
        this.viewElementDefinitionBuilder = viewElementDefinitionBuilder;
        return this;
    }

}
