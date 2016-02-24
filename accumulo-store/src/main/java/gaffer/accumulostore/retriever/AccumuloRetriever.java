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

package gaffer.accumulostore.retriever;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.IteratorSettingFactory;
import gaffer.accumulostore.key.RangeFactory;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.CloseableIterable;
import gaffer.accumulostore.utils.CloseableIterator;
import gaffer.data.element.Element;
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.operation.GetOperation;
import gaffer.store.StoreException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import java.util.Set;

public abstract class AccumuloRetriever<OP_TYPE extends GetOperation<?, ?>> implements CloseableIterable<Element> {
    private static final String AUTHORISATIONS_SEPERATOR = ",";
    protected CloseableIterator<Element> iterator;
    protected final AccumuloStore store;
    protected final Authorizations authorisations;
    protected final RangeFactory rangeFactory;
    protected final IteratorSettingFactory iteratorSettingFactory;
    protected final OP_TYPE operation;
    protected final AccumuloElementConverter elementConverter;
    protected final IteratorSetting[] iteratorSettings;

    protected AccumuloRetriever(final AccumuloStore store, final OP_TYPE operation,
                                final IteratorSetting... iteratorSettings) throws StoreException {
        this.store = store;
        this.rangeFactory = store.getKeyPackage().getRangeFactory();
        this.iteratorSettingFactory = store.getKeyPackage().getIteratorFactory();
        this.elementConverter = store.getKeyPackage().getKeyConverter();
        this.operation = operation;
        this.iteratorSettings = iteratorSettings;
        if (null != this.operation.getOption(AccumuloStoreConstants.OPERATION_AUTHORISATIONS)) {
            this.authorisations = new Authorizations(
                    this.operation.getOption(AccumuloStoreConstants.OPERATION_AUTHORISATIONS).split(AUTHORISATIONS_SEPERATOR));
        } else {
            this.authorisations = new Authorizations();
        }
    }

    /**
     * Performs any transformations specified in a view on an element
     *
     * @param element the element to transform
     */
    public void doTransformation(final Element element) {
        final ViewElementDefinition viewDef = operation.getView().getElement(element.getGroup());
        if (viewDef != null) {
            transform(element, viewDef.getTransformer());
        }
    }

    @Override
    public void close() {
        if (iterator != null) {
            iterator.close();
        }
    }

    /**
     * Create a scanner to use used in your query.
     * <p>
     *
     * @param ranges the ranges to get the scanner for
     * @return A {@link org.apache.accumulo.core.client.BatchScanner} for the
     * table specified in the properties with the ranges provided.
     * @throws TableNotFoundException if an accumulo table could not be found
     * @throws StoreException         if a connection to accumulo could not be created.
     */
    protected BatchScanner getScanner(final Set<Range> ranges) throws TableNotFoundException, StoreException {
        final BatchScanner scanner = store.getConnection().createBatchScanner(store.getProperties().getTable(),
                authorisations, store.getProperties().getThreadsForBatchScanner());
        if (iteratorSettings != null) {
            for (final IteratorSetting iteratorSetting : iteratorSettings) {
                if (iteratorSetting != null) {
                    scanner.addScanIterator(iteratorSetting);
                }
            }
        }
        scanner.setRanges(Range.mergeOverlapping(ranges));
        // Currently hard links element class to column family position.
        for (final String col : operation.getView().getEdgeGroups()) {
            scanner.fetchColumnFamily(new Text(col));
        }
        for (final String col : operation.getView().getEntityGroups()) {
            scanner.fetchColumnFamily(new Text(col));
        }
        return scanner;
    }

    protected void transform(final Element element, final ElementTransformer transformer) {
        if (transformer != null) {
            transformer.transform(element);
        }
    }
}
