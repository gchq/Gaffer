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
package uk.gov.gchq.gaffer.accumulostore.key.impl;

import uk.gov.gchq.gaffer.accumulostore.key.AbstractElementFilter;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.IteratorOptionsBuilder;
import uk.gov.gchq.gaffer.data.element.Element;

public class ElementPreAggregationFilter extends AbstractElementFilter {

    @Override
    protected boolean validate(final Element element) {
        return validator.validateInput(element);
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptionsBuilder(super.describeOptions()).addViewNamedOption().addSchemaNamedOption()
                .addElementConverterClassNamedOption().setIteratorName(AccumuloStoreConstants.ELEMENT_PRE_AGGREGATION_FILTER_ITERATOR_NAME)
                .setIteratorDescription("Only returns elements that pass validation against the given view").build();
    }
}
