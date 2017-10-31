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

package uk.gov.gchq.gaffer.store.operation.handler;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetTraits;

public class GetTraitsHandler implements OutputOperationHandler<GetTraits, Iterable<? extends StoreTrait>> {

    @Override
    public Iterable<? extends StoreTrait> doOperation(final GetTraits operation, final Context context, final Store store) throws OperationException {
        try {
            return operation.currentlyAvailableTraits()
                    ? store.getCurrentlyAvailableTraits(null)
                    : store.getTraits();
        } catch (final Exception e) {
            throw new OperationException("Error getting Traits. isSupportedTraits: " + operation.currentlyAvailableTraits(), e);
        }
    }
}
