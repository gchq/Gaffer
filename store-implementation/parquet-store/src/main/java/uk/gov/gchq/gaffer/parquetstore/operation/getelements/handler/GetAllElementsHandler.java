/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.impl.ParquetElementRetriever;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

/**
 *
 */
public class GetAllElementsHandler implements OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetAllElementsHandler.class);

    @Override
    public CloseableIterable<? extends Element> doOperation(final GetAllElements operation, final Context context, final Store store) throws OperationException {
        validateView(operation.getView());
        return runQuery(operation, (ParquetStore) store);
    }

    private CloseableIterable<Element> runQuery(final GetAllElements operation, final ParquetStore store) throws OperationException {
        try {
            return new ParquetElementRetriever(operation.getView(), store, operation.getDirectedType(), null, null, null);
        } catch (StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }
    }

    private void validateView(final View view) throws OperationException {
        for (final String group : view.getGroups()) {
            final ViewElementDefinition groupView = view.getElement(group);
            if (groupView.getPostAggregationFilter() != null) {
                throw new OperationException("The ParquetStore does not currently support post aggregation filters.");
            } else if (groupView.getPostTransformFilter() != null) {
                throw new OperationException("The ParquetStore does not currently support post transformation filters.");
            } else if (groupView.getTransformer() != null) {
                throw new OperationException("The ParquetStore does not currently support transformations.");
            }
        }
    }
}
