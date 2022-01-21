/*
 * Copyright 2022 Crown Copyright
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
import uk.gov.gchq.gaffer.store.operation.HasTrait;

import java.util.HashMap;
import java.util.Set;

import static java.util.Objects.isNull;


public class HasTraitHandler implements OutputOperationHandler<HasTrait, Boolean> {

    @Override
    public Boolean doOperation(final HasTrait operation, final Context context, final Store store) throws OperationException {
        final Set<StoreTrait> traits = store.execute(new GetTraits.Builder()
                .currentTraits(operation.isCurrentTraits())
                //deep copy options
                .options(isNull(operation.getOptions()) ? new HashMap<>() : new HashMap<>(operation.getOptions()))
                .build(), context
        );
        if (null == traits) {
            return false;
        }
        return traits.contains(operation.getTrait());
    }
}
