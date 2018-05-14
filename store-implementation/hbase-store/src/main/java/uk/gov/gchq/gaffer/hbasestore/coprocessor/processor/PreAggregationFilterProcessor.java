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
package uk.gov.gchq.gaffer.hbasestore.coprocessor.processor;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.hbasestore.serialisation.LazyElementCell;
import uk.gov.gchq.gaffer.store.ElementValidator;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

@Since("1.0.0")
@Summary("Filters out invalid elements before aggregation")
public class PreAggregationFilterProcessor extends FilterProcessor {
    private final ElementValidator validator;

    public PreAggregationFilterProcessor(final View view) {
        validator = new ElementValidator(view);
    }

    @Override
    public boolean test(final LazyElementCell elementCell) {
        return validator.validateInput(elementCell.getElement());
    }

    public View getView() {
        return validator.getView();
    }
}
