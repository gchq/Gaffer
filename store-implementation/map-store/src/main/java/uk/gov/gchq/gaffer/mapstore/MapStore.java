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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.mapstore.impl.AddElementsHandler;
import uk.gov.gchq.gaffer.mapstore.impl.CountAllElementsDefaultViewHandler;
import uk.gov.gchq.gaffer.mapstore.impl.GetAdjacentIdsHandler;
import uk.gov.gchq.gaffer.mapstore.impl.GetAllElementsHandler;
import uk.gov.gchq.gaffer.mapstore.impl.GetElementsHandler;
import uk.gov.gchq.gaffer.mapstore.impl.MapImpl;
import uk.gov.gchq.gaffer.mapstore.operation.CountAllElementsDefaultView;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * An implementation of {@link Store} that uses any class that implements Java's {@link java.util.Map} interface to
 * store the {@link Element}s. The {@link Element} objects are stored in memory, i.e . no serialisation is performed.
 * <p>
 * It is designed to support efficient aggregation of properties. The key of the Map is the {@link Element} with any
 * group-by properties, and the value is the non-group-by properties. This allows very quick aggregation of properties
 * from a new {@link Element} with existing properties.
 * </p>
 * <p>
 * Indices can optionally be maintained to allow quick look-up of {@link Element}s based on {@link EntityId}s
 * or {@link uk.gov.gchq.gaffer.data.element.id.EdgeId}s.
 * </p>
 */
public class MapStore extends Store {
    public static final Set<StoreTrait> TRAITS = new HashSet<>(Arrays.asList(
            StoreTrait.INGEST_AGGREGATION,
            StoreTrait.PRE_AGGREGATION_FILTERING,
            StoreTrait.POST_AGGREGATION_FILTERING,
            StoreTrait.TRANSFORMATION,
            StoreTrait.POST_TRANSFORMATION_FILTERING));
    private static final Logger LOGGER = LoggerFactory.getLogger(MapStore.class);
    private static MapImpl staticMapImpl;
    private MapImpl mapImpl;

    public static void resetStaticMap() {
        staticMapImpl = null;
    }

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        // Initialise store
        super.initialise(graphId, schema, properties);

        // Initialise maps
        mapImpl = createMapImpl();
    }

    public MapImpl getMapImpl() {
        return mapImpl;
    }

    @Override
    public Set<StoreTrait> getTraits() {
        return TRAITS;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be MapStoreProperties")
    @Override
    public MapStoreProperties getProperties() {
        return (MapStoreProperties) super.getProperties();
    }

    @Override
    protected Class<MapStoreProperties> getPropertiesClass() {
        return MapStoreProperties.class;
    }

    protected MapImpl createMapImpl() {
        if (getProperties().isStaticMap()) {
            LOGGER.debug("Using static map");
            if (null == staticMapImpl) {
                staticMapImpl = new MapImpl(getSchema(), getProperties());
            }

            return staticMapImpl;
        }

        return new MapImpl(getSchema(), getProperties());
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        addOperationHandler(CountAllElementsDefaultView.class, new CountAllElementsDefaultViewHandler());
    }

    @Override
    protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
        return new GetElementsHandler();
    }

    @Override
    protected OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getGetAllElementsHandler() {
        return new GetAllElementsHandler();
    }

    @Override
    protected OutputOperationHandler<GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler() {
        return new GetAdjacentIdsHandler();
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return Serialiser.class;
    }
}
