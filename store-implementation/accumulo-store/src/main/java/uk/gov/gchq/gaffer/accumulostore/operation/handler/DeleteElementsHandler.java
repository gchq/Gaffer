/*
 * Copyright 2024-2025 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.ValidatedElements;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.ArrayList;
import java.util.List;

public class DeleteElementsHandler implements OutputOperationHandler<DeleteElements, Long> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteElementsHandler.class);
    private Long elementCount = 0L;

    @Override
    public Long doOperation(final DeleteElements operation,
            final Context context, final Store store)
            throws OperationException {
        deleteElements(operation, (AccumuloStore) store);
        return elementCount;
    }

    private Long deleteElements(final DeleteElements operation, final AccumuloStore store)
            throws OperationException {
        try {
            final Iterable<? extends Element> validatedElements;
            if (operation.isValidate()) {
                validatedElements = new ValidatedElements(operation.getInput(), store.getSchema(),
                        operation.isSkipInvalidElements());
            } else {
                validatedElements = operation.getInput();
            }

            final List<String> elementsToDelete = new ArrayList<>();
            for (final Element el : validatedElements) {
                elementsToDelete.add(el.toString());
                elementCount++;
            }
            LOGGER.debug("Deleting elements: {}", elementsToDelete);

            store.deleteElements(validatedElements);
        } catch (final StoreException e) {
            throw new OperationException("Failed to delete elements", e);
        }

        return elementCount;
    }
}
