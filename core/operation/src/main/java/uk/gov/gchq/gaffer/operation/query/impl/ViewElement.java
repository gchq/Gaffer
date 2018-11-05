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

package uk.gov.gchq.gaffer.operation.query.impl;


import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.query.IFunction;
import uk.gov.gchq.gaffer.operation.query.IPredicate;
import uk.gov.gchq.gaffer.operation.query.IViewElement;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class ViewElement<OUT> extends ElementDirection<OUT> implements IViewElement {

    private String filterProp;
    private String propFrom;
    private String propTo;
    private boolean isGroupBy;

    private ElementFilter.Builder elementFilter;
    private ElementTransformer.Builder elementTransformer;


    public ViewElement(final OperationChain<OUT> outOperationChain) {
        super(outOperationChain);
        if (outOperationChain instanceof ViewElement) {
            ViewElement that = (ViewElement) outOperationChain;
            this.filterProp = that.filterProp;
            this.propFrom = that.propFrom;
            this.propTo = that.propTo;
            this.isGroupBy = that.isGroupBy;
            this.elementFilter = that.elementFilter;
            this.elementTransformer = that.elementTransformer;
        }
    }

    @Override
    public IPredicate filter(final String prop) {
        this.filterProp = prop;
        return new Predicate<>(this);
    }

    @Override
    public ViewElement groupBy(final String... propX) {

        ViewElementDefinition.Builder viewElementDefinitionBuilder;
        if (isNull(getViewElementDefinitionBuilder())) {
            viewElementDefinitionBuilder = new ViewElementDefinition.Builder();
            viewElementDefinitionBuilder(viewElementDefinitionBuilder);
        } else {
            viewElementDefinitionBuilder = getViewElementDefinitionBuilder();
        }

        viewElementDefinitionBuilder.groupBy(propX);

        //Resolve PreFilters before setting groupBy flag
        resolveElementFilter();

        isGroupBy = true;

        return this;
    }

    @Override
    public IFunction transform(final String propA, final String propB) {
        this.propFrom = propA;
        this.propTo = propB;

        return new Function(this);
    }

    protected String getFilterProp() {
        return filterProp;
    }

    protected ViewElement clearFilterProp() {
        this.filterProp = null;
        return this;
    }

    @Override
    protected void tidyUp() {
        if (nonNull(filterProp)) {
            if (isGroupBy) {
                throw new UnsupportedOperationException("PostFilter selection and function not complete");
            } else {
                throw new UnsupportedOperationException("PreFilter selection and function not complete");
            }
        }

        //Resolve postAggregate filters after groupBy flag.
        resolveElementFilter();

        resolveTransform();

        if (nonNull(propFrom) || nonNull(propTo)) {
            throw new UnsupportedOperationException("Transformer selection and function not complete");
        }

        clear();
        super.tidyUp();
    }

    private void resolveTransform() {
        if (nonNull(elementTransformer)) {
            ViewElementDefinition.Builder viewElementDefinitionBuilder = getViewElementDefinitionBuilder();
            if (isNull(viewElementDefinitionBuilder)) {
                viewElementDefinitionBuilder = new ViewElementDefinition.Builder();
                viewElementDefinitionBuilder(viewElementDefinitionBuilder);
            }

            viewElementDefinitionBuilder.transformer(elementTransformer.build());
            elementTransformer = null;
        }
    }

    private void resolveElementFilter() {
        if (nonNull(elementFilter)) {
            ViewElementDefinition.Builder viewElementDefinitionBuilder = getViewElementDefinitionBuilder();
            if (isNull(viewElementDefinitionBuilder)) {
                viewElementDefinitionBuilder = new ViewElementDefinition.Builder();
                viewElementDefinitionBuilder(viewElementDefinitionBuilder);
            }
            if (getIsGroupBy()) {
                viewElementDefinitionBuilder.postAggregationFilter(elementFilter.build());
            } else {
                viewElementDefinitionBuilder.preAggregationFilter(elementFilter.build());
            }
            elementFilter = null;
        }
    }


    private void clear() {
        filterProp = null;
        isGroupBy = false;
        propFrom = null;
        propTo = null;
        elementFilter = null;
        elementTransformer = null;
    }

    protected String getPropFrom() {
        return propFrom;
    }

    protected ViewElement<OUT> clearPropFrom() {
        this.propFrom = null;
        return this;
    }

    protected String getPropTo() {
        return propTo;
    }

    protected ViewElement<OUT> clearPropTo() {
        this.propTo = null;
        return this;
    }

    protected ViewElement<OUT> clearIsGroupBy() {
        this.isGroupBy = false;
        return this;
    }

    protected Boolean getIsGroupBy() {
        return isGroupBy;
    }

    protected ElementFilter.Builder getElementFilter() {
        return elementFilter;
    }

    protected void elementFilter(final ElementFilter.Builder elementFilter) {
        this.elementFilter = elementFilter;
    }

    protected ElementTransformer.Builder getElementTransformer() {
        return elementTransformer;
    }

    protected void elementTransformer(final ElementTransformer.Builder elementTransformer) {
        this.elementTransformer = elementTransformer;
    }
}

