package gaffer.example;

import gaffer.CloseableIterable;
import gaffer.GraphAccessException;
import gaffer.accumulo.AccumuloBackedGraph;
import gaffer.accumulo.TableUtils;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.TypeValue;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.SimpleTimeZone;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;

/**
 * A simple example of adding data to a Gaffer instance, and executing some queries.
 */
public class Example {

    private static final String INSTORE = "instore";
    private static final String PUBLIC = "public";
    private static final String ONLINE = "online";
    private static final String PURCHASE = "purchase";
    private static final String PRODUCT = "product";
    private static final String CUSTOMER = "customer";
    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
    static {
        DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
    }
    private final static Date START_JAN_1ST = toDate("20150101000000");
    private final static Date START_JAN_2ND = toDate("20150102000000");
    private final static Date START_JAN_3RD = toDate("20150103000000");

    private static Date toDate(final String date) {
        try {
            return DATE_FORMAT.parse(date);
        } catch (final ParseException e) {
            // Ignore
        }
        return null;
    }

    public static void main(final String[] args) throws IOException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException,
            TableNotFoundException, GraphAccessException {
        // Create mock Accumulo instance
        final Instance instance = new MockInstance();
        final Connector connector = instance.getConnector("user", new PasswordToken("password"));
        connector.securityOperations().changeUserAuthorizations("user", new Authorizations(PUBLIC));

        // Create Gaffer table
        final String tableName = "example";
        final long ageOffTimeInMilliseconds = Long.MAX_VALUE; // Set to no age-off for this example
        TableUtils.createTable(connector, tableName, ageOffTimeInMilliseconds);

        // Create AccumuloBackedGraph for adding and querying data
        final AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, tableName);

        // Add initial data
        graph.addGraphElementsWithStatistics(getData());

        // Query for customer B
        System.out.println("Querying for customer B");
        CloseableIterable<GraphElementWithStatistics> iter = graph.getGraphElementsWithStatistics(new TypeValue(CUSTOMER, "B"));
        for (final GraphElementWithStatistics gews : iter) {
            System.out.println(gews);
        }
        iter.close();
        System.out.println();

        // See a new observation of customer|B to product|Q. Create Edge and two Entities to add.
        System.out.println("Adding edge customer|B -> product|Q, and corresponding entities\n");
        final Edge edge = new Edge(CUSTOMER, "B", PRODUCT, "Q", PURCHASE, INSTORE, true, PUBLIC, START_JAN_2ND, START_JAN_3RD);
        final SetOfStatistics edgeStatistics = new SetOfStatistics();
        edgeStatistics.addStatistic("count", new Count(1));
        final Entity entityB = new Entity(CUSTOMER, "B", PURCHASE, INSTORE, PUBLIC, START_JAN_2ND, START_JAN_3RD);
        final SetOfStatistics entityBStatistics = new SetOfStatistics();
        entityBStatistics.addStatistic("count", new Count(1));
        final Entity entityQ = new Entity(PRODUCT, "Q", PURCHASE, INSTORE, PUBLIC, START_JAN_2ND, START_JAN_3RD);
        final SetOfStatistics entityQStatistics = new SetOfStatistics();
        entityQStatistics.addStatistic("count", new Count(1));
        final Set<GraphElementWithStatistics> newData = new HashSet<GraphElementWithStatistics>();
        newData.add(new GraphElementWithStatistics(new GraphElement(edge), edgeStatistics));
        newData.add(new GraphElementWithStatistics(new GraphElement(entityB), entityBStatistics));
        newData.add(new GraphElementWithStatistics(new GraphElement(entityQ), entityQStatistics));
        graph.addGraphElementsWithStatistics(newData);

        // Query for customer B again - note that counts have increased
        System.out.println("Querying for customer B again - note that counts have increased");
        iter = graph.getGraphElementsWithStatistics(new TypeValue(CUSTOMER, "B"));
        for (final GraphElementWithStatistics gews : iter) {
            System.out.println(gews);
        }
        iter.close();
        System.out.println();

        // Query for customer B, but only get entity information (this can be much quicker than retrieving all the edges)
        System.out.println("Querying for customer B again, but only retrieving entities");
        graph.setReturnEntitiesOnly();
        iter = graph.getGraphElementsWithStatistics(new TypeValue(CUSTOMER, "B"));
        for (final GraphElementWithStatistics gews : iter) {
            System.out.println(gews);
        }
        iter.close();
        graph.setReturnEntitiesAndEdges();
        System.out.println();

        // Query for customer A on January 1st (i.e. from midnight at start of Jan 1st to midnight at start of Jan 2nd)
        System.out.println("Querying for customer A on January 1st");
        graph.setTimeWindow(START_JAN_1ST, START_JAN_2ND);
        iter = graph.getGraphElementsWithStatistics(new TypeValue(CUSTOMER, "A"));
        for (final GraphElementWithStatistics gews : iter) {
            System.out.println(gews);
        }
        iter.close();
        System.out.println();

        // Query for customer A on January 2nd (i.e. from midnight at start of Jan 2nd to midnight at start of Jan 3rd)
        System.out.println("Querying for customer A on January 2nd");
        graph.setTimeWindow(START_JAN_2ND, START_JAN_3RD);
        iter = graph.getGraphElementsWithStatistics(new TypeValue(CUSTOMER, "A"));
        for (final GraphElementWithStatistics gews : iter) {
            System.out.println(gews);
        }
        iter.close();
        System.out.println();

        // Query for customer A over all time
        System.out.println("Querying for customer A over all time - note that counts from different days are summed");
        graph.setTimeWindowToAllTime();
        iter = graph.getGraphElementsWithStatistics(new TypeValue(CUSTOMER, "A"));
        for (final GraphElementWithStatistics gews : iter) {
            System.out.println(gews);
        }
        iter.close();
    }

    private static Set<GraphElementWithStatistics> getData() {
        final Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();

        addEdgeData(data, CUSTOMER, "A", PRODUCT, "P", PURCHASE, ONLINE, true, PUBLIC, START_JAN_1ST, START_JAN_2ND, 1);
        addEdgeData(data, CUSTOMER, "A", PRODUCT, "P", PURCHASE, ONLINE, true, PUBLIC, START_JAN_2ND, START_JAN_3RD, 2);
        addEdgeData(data, CUSTOMER, "A", PRODUCT, "P", PURCHASE, INSTORE, true, PUBLIC, START_JAN_2ND, START_JAN_3RD, 17);
        addEdgeData(data, CUSTOMER, "A", PRODUCT, "P", PURCHASE, INSTORE, false, PUBLIC, START_JAN_2ND, START_JAN_3RD, 123456);
        addEdgeData(data, CUSTOMER, "B", PRODUCT, "Q", PURCHASE, INSTORE, true, PUBLIC, START_JAN_2ND, START_JAN_3RD, 99);
        addEntityData(data, CUSTOMER, "A", PURCHASE, ONLINE, PUBLIC, START_JAN_1ST, START_JAN_2ND, 1000000);
        addEntityData(data, CUSTOMER, "B", PURCHASE, INSTORE, PUBLIC, START_JAN_1ST, START_JAN_2ND, 99);
        addEntityData(data, PRODUCT, "R", PURCHASE, INSTORE, PUBLIC, START_JAN_1ST, START_JAN_2ND, 99);

        return data;
    }

    private static void addEdgeData(final Set<GraphElementWithStatistics> data, final String sourceType, final String sourceValue, final String destType,
            final String destValue, final String summaryType, final String summarySubType, final boolean directed, final String visibility, final Date start,
            final Date end, final int count) {
        final Edge edge = new Edge(sourceType, sourceValue, destType, destValue, summaryType, summarySubType, directed, visibility, start, end);
        data.add(new GraphElementWithStatistics(new GraphElement(edge), stats(count)));

    }

    private static void addEntityData(final Set<GraphElementWithStatistics> data, final String entityType, final String entityValue, final String summaryType,
            final String summarySubType, final String visibility, final Date start,
            final Date end, final int count) {
        final Entity entity = new Entity(entityType, entityValue, summaryType, summarySubType, visibility, start, end);
        data.add(new GraphElementWithStatistics(new GraphElement(entity), stats(count)));
    }

    private static SetOfStatistics stats(final int count) {
        final SetOfStatistics statisticsEntity = new SetOfStatistics();
        statisticsEntity.addStatistic("count", new Count(count));
        return statisticsEntity;
    }
}
