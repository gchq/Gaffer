package gaffer.accumulostore.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import gaffer.accumulostore.utils.AccumuloPropertyNames;
import gaffer.commonutil.CommonConstants;
import gaffer.commonutil.StreamUtil;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestTypes;
import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.exception.SerialisationException;
import gaffer.function.simple.aggregate.StringConcat;
import gaffer.graph.Graph;
import gaffer.graph.Graph.Builder;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.serialisation.AbstractSerialisation;
import gaffer.serialisation.implementation.StringSerialiser;
import gaffer.store.StoreProperties;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEntityDefinition;
import gaffer.store.schema.TypeDefinition;
import gaffer.user.User;
import org.junit.Test;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AccumuloVisibilityIT {

    private static final StoreProperties STORE_PROPERTIES
            = StoreProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreITs.class));
    private static final User USER_DEFAULT = new User();
    private static final User USER_VIS_1 = new User.Builder().dataAuth("vis1")
                                                             .build();
    private static final User USER_VIS_2 = new User.Builder().dataAuth("vis2")
                                                             .build();

    @Test
    public void shouldAccessMissingVisibilityGroups() throws OperationException, JsonProcessingException {

        final Graph graph = createGraph();

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");

        // Do NOT add an explicit visibility property
        // entity1.putProperty(AccumuloPropertyNames.VISIBILITY, "");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, USER_DEFAULT);

        final GetRelatedElements<EntitySeed, Element> get = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .build();

        final CloseableIterable<Element> iterable = graph.execute(get, USER_DEFAULT);

        final List<Element> results = Lists.newArrayList(iterable);

        // Check for all entities which should be visible
        assertThat("Results do not contain all expected entities.", results, hasSize(1));

        for (final Element e : results) {

            // Check that all visible entities contain the visibility property
            assertTrue("Visibility property should be visible.", e.getProperties()
                                                                  .containsKey(AccumuloPropertyNames.VISIBILITY));

            assertThat("Visibility property should contain an empty String.", e.getProperties()
                                                                               .get(AccumuloPropertyNames.VISIBILITY)
                                                                               .toString(), isEmptyString());
        }

        iterable.close();
    }

    @Test
    public void shouldAccessMissingVisibilityGroupsWithNoVisibilityPropertyInSchema() throws OperationException, JsonProcessingException {

        final Graph graph = createGraphNoVisibility();

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, USER_DEFAULT);

        final GetRelatedElements<EntitySeed, Element> get = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .build();

        final CloseableIterable<Element> iterable = graph.execute(get, USER_DEFAULT);

        final List<Element> results = Lists.newArrayList(iterable);

        // Check for all entities which should be visible
        assertThat("Results do not contain all expected entities.", results, hasSize(1));

        for (final Element e : results) {

            // Check that all visible entities do not contain the visibility property
            assertFalse("Visibility property should not be visible.", e.getProperties()
                                                                       .containsKey(AccumuloPropertyNames.VISIBILITY));
        }

        iterable.close();
    }

    @Test
    public void shouldAccessEmptyVisibilityGroups() throws OperationException, JsonProcessingException {

        final Graph graph = createGraph();

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");
        entity1.putProperty(AccumuloPropertyNames.VISIBILITY, "");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, USER_DEFAULT);

        final GetRelatedElements<EntitySeed, Element> get = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .build();

        final CloseableIterable<Element> iterable = graph.execute(get, USER_DEFAULT);

        final List<Element> results = Lists.newArrayList(iterable);

        // Check for all entities which should be visible
        assertThat("Results do not contain all expected entities.", results, hasSize(1));

        for (final Element e : results) {

            // Check that all visible entities contain the visibility property
            assertTrue("Visibility property should be visible.", e.getProperties()
                                                                  .containsKey(AccumuloPropertyNames.VISIBILITY));

            assertThat("Visibility property should contain an empty String.", e.getProperties()
                                                                               .get(AccumuloPropertyNames.VISIBILITY)
                                                                               .toString(), isEmptyString());
        }

        iterable.close();
    }

    @Test
    public void shouldAccessNullVisibilityGroups() throws OperationException, JsonProcessingException {

        final Graph graph = createGraph();

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");
        entity1.putProperty(AccumuloPropertyNames.VISIBILITY, null);
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, USER_DEFAULT);

        final GetRelatedElements<EntitySeed, Element> get = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("B"))
                .build();

        final CloseableIterable<Element> iterable = graph.execute(get, USER_DEFAULT);

        final List<Element> results = Lists.newArrayList(iterable);

        // Check for all entities which should be visible
        assertThat("Results do not contain all expected entities.", results, hasSize(1));

        for (final Element e : results) {

            // Check that all visible entities contain the visibility property
            assertTrue("Visibility property should be visible.", e.getProperties()
                                                                  .containsKey(AccumuloPropertyNames.VISIBILITY));

            assertThat("Visibility property should contain an empty String.", e.getProperties()
                                                                               .get(AccumuloPropertyNames.VISIBILITY)
                                                                               .toString(), isEmptyString());
        }

        iterable.close();
    }

    @Test
    public void shouldAccessSingleVisibilityGroup() throws OperationException {

        final Graph graph = createGraph();

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");
        entity1.putProperty(AccumuloPropertyNames.VISIBILITY, "vis1");
        final Entity entity2 = new Entity(TestGroups.ENTITY, "B");
        entity2.putProperty(AccumuloPropertyNames.VISIBILITY, "vis1");

        elements.add(entity1);
        elements.add(entity2);

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, USER_VIS_1);

        final GetRelatedElements<EntitySeed, Element> get = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("B"))
                .build();

        final CloseableIterable<Element> userVis1Iterable = graph.execute(get, USER_VIS_1);
        final CloseableIterable<Element> userVis2Iterable = graph.execute(get, USER_VIS_2);

        final List<Element> userVis1Results = Lists.newArrayList(userVis1Iterable);
        final List<Element> userVis2Results = Lists.newArrayList(userVis2Iterable);

        assertThat(userVis1Results, hasSize(2));
        assertThat(userVis2Results, is(empty()));

        for (final Element e : userVis1Results) {
            // Check that all visible entities contain the visibility property
            assertTrue("Missing visibility property.", e.getProperties()
                                                        .containsKey(AccumuloPropertyNames.VISIBILITY));

            // Check that the visibility key contains the correct value
            assertEquals("Visibility property should be \"vis1\"",
                    e.getProperties()
                     .get(AccumuloPropertyNames.VISIBILITY)
                     .toString(), "vis1");
        }

        userVis1Iterable.close();
        userVis2Iterable.close();
    }

    @Test
    public void shouldAccessMultipleVisibilityGroups_and() throws OperationException {

        final Graph graph = createGraph();

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "B");
        entity1.putProperty(AccumuloPropertyNames.VISIBILITY, "vis1&vis2");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, new User());

        final GetRelatedElements<EntitySeed, Element> get = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("B"))
                .build();

        final CloseableIterable<Element> iterable = graph.execute(get, new User(User.UNKNOWN_USER_ID, Sets
                .newHashSet("vis1", "vis2")));

        final List<Element> results = Lists.newArrayList(iterable);

        assertThat("Results do not contain all expected Elements.", results, hasSize(1));

        for (final Element e : iterable) {
            assertTrue(e.getProperties()
                        .containsKey(AccumuloPropertyNames.VISIBILITY));
        }

        iterable.close();
    }

    @Test
    public void shouldAccessMultipleVisibilityGroups_or() throws OperationException {

        final Graph graph = createGraph();

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "B");
        entity1.putProperty(AccumuloPropertyNames.VISIBILITY, "vis1|vis2");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, new User());

        final GetRelatedElements<EntitySeed, Element> get = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("B"))
                .build();
        final CloseableIterable<Element> iterable = graph.execute(get, new User(User.UNKNOWN_USER_ID, Sets
                .newHashSet("vis1")));

        final List<Element> results = Lists.newArrayList(iterable);

        assertThat("Results do not contain all expected Elements.", results, hasSize(1));

        for (final Element e : results) {
            assertTrue(e.getProperties()
                        .containsKey(AccumuloPropertyNames.VISIBILITY));
        }

        iterable.close();
    }

    private Graph createGraph() {
        return new Builder()
                .storeProperties(STORE_PROPERTIES)
                .addSchema(new Schema.Builder()
                        .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                                .clazz(String.class)
                                .build())
                        .type("colQual", new TypeDefinition.Builder()
                                .clazz(String.class)
                                .aggregateFunction(new StringConcat())
                                .serialiser(new StringSerialiser())
                                .build())
                        .type(AccumuloPropertyNames.VISIBILITY, new TypeDefinition.Builder()
                                .clazz(String.class)
                                .aggregateFunction(new StringConcat())
                                .serialiser(new VisibilitySerialiser())
                                .build())
                        .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                                .vertex(TestTypes.ID_STRING)
                                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "colQual")
                                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "colQual")
                                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "colQual")
                                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "colQual")
                                .property(AccumuloPropertyNames.VISIBILITY, AccumuloPropertyNames.VISIBILITY)
                                .groupBy(AccumuloPropertyNames.COLUMN_QUALIFIER,
                                        AccumuloPropertyNames.COLUMN_QUALIFIER_2,
                                        AccumuloPropertyNames.COLUMN_QUALIFIER_3,
                                        AccumuloPropertyNames.COLUMN_QUALIFIER_4)
                                .build())
                        .visibilityProperty(AccumuloPropertyNames.VISIBILITY)
                        .build())
                .build();
    }

    private Graph createGraphNoVisibility() {
        return new Builder()
                .storeProperties(STORE_PROPERTIES)
                .addSchema(new Schema.Builder()
                        .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                                .clazz(String.class)
                                .build())
                        .type("colQual", new TypeDefinition.Builder()
                                .clazz(String.class)
                                .aggregateFunction(new StringConcat())
                                .serialiser(new StringSerialiser())
                                .build())
                        .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                                .vertex(TestTypes.ID_STRING)
                                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "colQual")
                                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "colQual")
                                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "colQual")
                                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "colQual")
                                .groupBy(AccumuloPropertyNames.COLUMN_QUALIFIER,
                                        AccumuloPropertyNames.COLUMN_QUALIFIER_2,
                                        AccumuloPropertyNames.COLUMN_QUALIFIER_3,
                                        AccumuloPropertyNames.COLUMN_QUALIFIER_4)
                                .build())
                        .build())
                .build();
    }

    public static final class VisibilitySerialiser extends AbstractSerialisation<String> {

        @Override
        public boolean canHandle(final Class clazz) {
            return String.class.equals(clazz);
        }

        @Override
        public byte[] serialise(final String value) throws SerialisationException {
            try {
                return value.getBytes(CommonConstants.UTF_8);
            } catch (UnsupportedEncodingException e) {
                throw new SerialisationException(e.getMessage(), e);
            }
        }

        @Override
        public String deserialise(final byte[] bytes) throws SerialisationException {
            try {
                return new String(bytes, CommonConstants.UTF_8);
            } catch (UnsupportedEncodingException e) {
                throw new SerialisationException(e.getMessage(), e);
            }
        }

        @Override
        public byte[] serialiseNull() {
            return new byte[]{};
        }

        @Override
        public String deserialiseEmptyBytes() {
            return "";
        }

        @Override
        public boolean isByteOrderPreserved() {
            return true;
        }
    }

}
