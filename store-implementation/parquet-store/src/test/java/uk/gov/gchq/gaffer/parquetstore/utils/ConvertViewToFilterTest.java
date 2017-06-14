package uk.gov.gchq.gaffer.parquetstore.utils;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.SerialisationFactory;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;

import java.io.IOException;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class ConvertViewToFilterTest {

    private SchemaUtils schemaUtils;

    @Before
    public void setUp() throws StoreException {
        Logger.getRootLogger().setLevel(Level.WARN);
        final Schema schema = Schema.fromJson(getClass().getResourceAsStream("/schemaUsingStringVertexType/dataSchema.json"),
                getClass().getResourceAsStream("/schemaUsingStringVertexType/dataTypes.json"),
                getClass().getResourceAsStream("/schemaUsingStringVertexType/storeSchema.json"),
                getClass().getResourceAsStream("/schemaUsingStringVertexType/storeTypes.json"));
        final SchemaOptimiser optimiser = new SchemaOptimiser(new SerialisationFactory(ParquetStoreConstants.SERIALISERS));
        this.schemaUtils = new SchemaUtils(optimiser.optimise(schema, true));
    }

    @After
    public void cleanUp() {
        this.schemaUtils = null;
    }

    @Test
    public void getBasicGroupFilterTest() throws OperationException, SerialisationException {
        final View view = new View.Builder().entity("BasicEntity",
                new ViewElementDefinition.Builder().preAggregationFilter(
                        new ElementFilter.Builder()
                                .select("property2")
                                .execute(
                                new IsEqual(2.0))
                                .build())
                        .build())
                .build();
        final FilterPredicate filter = ParquetFilterUtils.buildGroupFilter(view, this.schemaUtils, "BasicEntity", DirectedType.EITHER, true).get0();
        final FilterPredicate expected = eq(doubleColumn("property2"), 2.0);
        assertEquals(expected, filter);
    }

    @Test
    public void getMultiColumnGroupFilterTest() throws OperationException, IOException {
        final HyperLogLogPlus hllp = new HyperLogLogPlus(5,5);
        hllp.offer("test1");
        hllp.offer("test2");

        final View view = new View.Builder().entity("BasicEntity",
                new ViewElementDefinition.Builder().preAggregationFilter(
                        new ElementFilter.Builder()
                                .select("property4")
                                .execute(
                                        new IsEqual(hllp))
                                .build())
                        .build())
                .build();
        final FilterPredicate filter = ParquetFilterUtils.buildGroupFilter(view, this.schemaUtils, "BasicEntity", DirectedType.EITHER, true).get0();
        final FilterPredicate expected = and(eq(binaryColumn("property4_raw_bytes"), Binary.fromReusedByteArray(hllp.getBytes())), eq(longColumn("property4_cardinality"), 2L));
        assertEquals(expected, filter);
    }

    @Test
    public void getNestedGroupFilterTest() throws OperationException, IOException {
        final HyperLogLogPlus hllp = new HyperLogLogPlus(5,5);
        hllp.offer("test1");
        hllp.offer("test2");

        final View view = new View.Builder().entity("BasicEntity",
                new ViewElementDefinition.Builder().preAggregationFilter(
                        new ElementFilter.Builder()
                                .select("property8")
                                .execute(
                                        new IsEqual(hllp))
                                .build())
                        .build())
                .build();
        final FilterPredicate filter = ParquetFilterUtils.buildGroupFilter(view, this.schemaUtils, "BasicEntity", DirectedType.EITHER, true).get0();
        final FilterPredicate expected = and(eq(binaryColumn("property8.raw_bytes"), Binary.fromReusedByteArray(hllp.getBytes())), eq(longColumn("property8.cardinality"), 2L));
        assertEquals(expected, filter);
    }
}
