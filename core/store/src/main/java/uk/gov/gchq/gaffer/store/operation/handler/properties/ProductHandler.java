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
package uk.gov.gchq.gaffer.store.operation.handler.properties;

import com.google.common.collect.Iterables;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.properties.Product;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

public class ProductHandler implements OutputOperationHandler<Product, Long> {
    @Override
    public Long doOperation(final Product operation, final Context context, final Store store) throws OperationException {

        if (null == operation.getInput() || Iterables.isEmpty(operation.getInput())) {
            return null;
        }

        return Streams.toStream(operation.getInput())
                      .filter(e -> null != e.getProperty(operation.getPropertyName()))
                      .reduce(1L, (acc, e) -> acc * ((Number) e.getProperty(operation
                                      .getPropertyName())).longValue(),
                              (a, b) -> a * b);
    }
}
