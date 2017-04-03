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
package uk.gov.gchq.gaffer.mapstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.mapstore.impl.AddElementsHandler;
import uk.gov.gchq.gaffer.mapstore.impl.CountAllElementsDefaultViewHandler;
import uk.gov.gchq.gaffer.mapstore.impl.GetAdjacentEntitySeedsHandler;
import uk.gov.gchq.gaffer.mapstore.impl.GetAllElementsHandler;
import uk.gov.gchq.gaffer.mapstore.impl.GetElementsHandler;
import uk.gov.gchq.gaffer.mapstore.impl.MapImpl;
import uk.gov.gchq.gaffer.mapstore.operation.CountAllElementsDefaultView;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * An implementation of {@link Store} that uses any class that implements Java's {@link java.util.Map} interface to
 * store the {@link Element}s. The {@link Element} objects are stored in memory, i.e . no serialisation is performed.
 * <p>
 * <p>It is designed to support efficient aggregation of properties. The key of the Map is the {@link Element} with any
 * group-by properties, and the value is the non-group-by properties. This allows very quick aggregation of properties
 * from a new {@link Element} with existing properties.
 * <p>
 * <p>Indices can optionally be maintained to allow quick look-up of {@link Element}s based on {@link EntitySeed}s
 * or {@link uk.gov.gchq.gaffer.operation.data.EdgeSeed}s.
 */
public class MapStore extends Store {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapStore.class);
    private static final Set<StoreTrait> TRAITS = new HashSet<>(Arrays.asList(
            StoreTrait.STORE_AGGREGATION,
            StoreTrait.PRE_AGGREGATION_FILTERING,
            StoreTrait.POST_AGGREGATION_FILTERING,
            StoreTrait.TRANSFORMATION,
            StoreTrait.POST_TRANSFORMATION_FILTERING));
    private MapImpl mapImpl;

    @Override
    public void initialise(final Schema schema, final StoreProperties storeProperties) throws StoreException {
        if (!(storeProperties instanceof MapStoreProperties)) {
            throw new StoreException("storeProperties must be an instance of MapStoreProperties");
        }
        // Initialise store
        final MapStoreProperties mapStoreProperties = (MapStoreProperties) storeProperties;
        super.initialise(schema, mapStoreProperties);
        // Initialise maps
        mapImpl = new MapImpl(schema, mapStoreProperties);
        LOGGER.info("Initialised MapStore");
    }

    public MapImpl getMapImpl() {
        return mapImpl;
    }

    @Override
    public Set<StoreTrait> getTraits() {
        return TRAITS;
    }

    @Override
    public boolean isValidationRequired() {
        return false;
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        addOperationHandler(CountAllElementsDefaultView.class, new CountAllElementsDefaultViewHandler());
    }

    @Override
    protected OperationHandler<GetElements<ElementSeed, Element>, CloseableIterable<Element>> getGetElementsHandler() {
        return new GetElementsHandler();
    }

    @Override
    protected OperationHandler<GetAllElements<Element>, CloseableIterable<Element>> getGetAllElementsHandler() {
        return new GetAllElementsHandler();
    }

    @Override
    protected OperationHandler<? extends GetAdjacentEntitySeeds, CloseableIterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
        return new GetAdjacentEntitySeedsHandler();
    }

    @Override
    protected OperationHandler<? extends AddElements, Void> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    @Override
    protected <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation, final Context context) {
        throw new UnsupportedOperationException("Operation " + operation.getClass() + " is not supported");
    }
}
