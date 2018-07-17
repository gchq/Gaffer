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

package uk.gov.gchq.gaffer.graph.hook.migrate.predicate;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.koryphe.predicate.KoryphePredicate;

public class TransformAndFilter extends KoryphePredicate<Element> {
    private ElementTransformer transformer;
    private ElementFilter filter;

    public TransformAndFilter() {
    }

    public TransformAndFilter(final ElementFilter filter) {
        this.filter = filter;
    }

    public TransformAndFilter(final ElementTransformer transformer, final ElementFilter filter) {
        this.transformer = transformer;
        this.filter = filter;
    }

    @Override
    public boolean test(final Element element) {
        if (null == element) {
            return false;
        }

        if (null == filter) {
            return true;
        }

        return filter.test(null != transformer ? transformer.apply(element.shallowClone()) : element);
    }

    public ElementTransformer getTransformer() {
        return transformer;
    }

    public void setTransformer(final ElementTransformer transformer) {
        this.transformer = transformer;
    }

    public ElementFilter getFilter() {
        return filter;
    }

    public void setFilter(final ElementFilter filter) {
        this.filter = filter;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("transformer", transformer)
                .append("filter", filter)
                .toString();
    }
}
