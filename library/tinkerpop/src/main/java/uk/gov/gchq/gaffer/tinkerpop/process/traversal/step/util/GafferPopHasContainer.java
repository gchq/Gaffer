/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import uk.gov.gchq.gaffer.tinkerpop.process.traversal.util.GafferCustomTypeFactory;
import uk.gov.gchq.gaffer.tinkerpop.process.traversal.util.GafferPredicateFactory;

import java.util.function.Predicate;

/**
 * Wrapper class for Gremlin {@link HasContainer}.
 *
 * Uses Predicates that have been converted for use in Gaffer.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GafferPopHasContainer extends HasContainer {

    final Predicate gafferPredicate;

    public GafferPopHasContainer(final HasContainer original) {
        super(original.getKey(), original.getPredicate());
        gafferPredicate = GafferPredicateFactory.convertGremlinPredicate(getPredicate());
    }

    public Predicate getGafferPredicate() {
        return gafferPredicate;
    }

    @Override
    protected boolean testId(final Element element) {
        return gafferPredicate.test(GafferCustomTypeFactory.parseAsCustomTypeIfValid(element.id()));
    }

    @Override
    protected boolean testIdAsString(final Element element) {
        return gafferPredicate.test(element.id().toString());
    }

    @Override
    protected boolean testLabel(final Element element) {
        return gafferPredicate.test(element.label());
    }

    @Override
    protected boolean testValue(final Property property) {
        return gafferPredicate.test(GafferCustomTypeFactory.parseAsCustomTypeIfValid(property.value()));
    }

    @Override
    protected boolean testKey(final Property property) {
        return gafferPredicate.test(property.key());
    }
}
