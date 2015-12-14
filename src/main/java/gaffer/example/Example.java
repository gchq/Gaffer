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
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * A simple example of adding data to a Gaffer instance, and executing some queries.
 */
public class Example {

    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
    static {
        DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
    }
    private static Date startJan1st;
    private static Date startJan2nd;
    private static Date startJan3rd;

    static {
        try {
            startJan1st = DATE_FORMAT.parse("20150101000000");
            startJan2nd = DATE_FORMAT.parse("20150102000000");
            startJan3rd = DATE_FORMAT.parse("20150103000000");
        } catch (ParseException e) {
            // Ignore
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, GraphAccessException {
        // Create mock Accumulo instance
        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("user", new PasswordToken("password"));
        connector.securityOperations().changeUserAuthorizations("user", new Authorizations("public"));

        // Create Gaffer table
        String tableName = "example";
        long ageOffTimeInMilliseconds = Long.MAX_VALUE; // Set to no age-off for this example
        TableUtils.createTable(connector, tableName, ageOffTimeInMilliseconds);

        // Create AccumuloBackedGraph for adding and querying data
        AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, tableName);

        // Add initial data
        graph.addGraphElementsWithStatistics(getData());

        // Query for customer B
        System.out.println("Querying for customer B");
        CloseableIterable<GraphElementWithStatistics> iter = graph.getGraphElementsWithStatistics(new TypeValue("customer", "B"));
        for (GraphElementWithStatistics gews : iter) {
            System.out.println(gews);
        }
        iter.close();
        System.out.println();

        // See a new observation of customer|B to product|Q. Create Edge and two Entities to add.
        System.out.println("Adding edge customer|B -> product|Q, and corresponding entities\n");
        Edge edge = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, "public", startJan2nd, startJan3rd);
        SetOfStatistics edgeStatistics = new SetOfStatistics();
        edgeStatistics.addStatistic("count", new Count(1));
        Entity entityB = new Entity("customer", "B", "purchase", "instore", "public", startJan2nd, startJan3rd);
        SetOfStatistics entityBStatistics = new SetOfStatistics();
        entityBStatistics.addStatistic("count", new Count(1));
        Entity entityQ = new Entity("product", "Q", "purchase", "instore", "public", startJan2nd, startJan3rd);
        SetOfStatistics entityQStatistics = new SetOfStatistics();
        entityQStatistics.addStatistic("count", new Count(1));
        Set<GraphElementWithStatistics> newData = new HashSet<GraphElementWithStatistics>();
        newData.add(new GraphElementWithStatistics(new GraphElement(edge), edgeStatistics));
        newData.add(new GraphElementWithStatistics(new GraphElement(entityB), entityBStatistics));
        newData.add(new GraphElementWithStatistics(new GraphElement(entityQ), entityQStatistics));
        graph.addGraphElementsWithStatistics(newData);

        // Query for customer B again - note that counts have increased
        System.out.println("Querying for customer B again - note that counts have increased");
        iter = graph.getGraphElementsWithStatistics(new TypeValue("customer", "B"));
        for (GraphElementWithStatistics gews : iter) {
            System.out.println(gews);
        }
        iter.close();
        System.out.println();

        // Query for customer B, but only get entity information (this can be much quicker than retrieving all the edges)
        System.out.println("Querying for customer B again, but only retrieving entities");
        graph.setReturnEntitiesOnly();
        iter = graph.getGraphElementsWithStatistics(new TypeValue("customer", "B"));
        for (GraphElementWithStatistics gews : iter) {
            System.out.println(gews);
        }
        iter.close();
        graph.setReturnEntitiesAndEdges();
        System.out.println();

        // Query for customer A on January 1st (i.e. from midnight at start of Jan 1st to midnight at start of Jan 2nd)
        System.out.println("Querying for customer A on January 1st");
        graph.setTimeWindow(startJan1st, startJan2nd);
        iter = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
        for (GraphElementWithStatistics gews : iter) {
            System.out.println(gews);
        }
        iter.close();
        System.out.println();

        // Query for customer A on January 2nd (i.e. from midnight at start of Jan 2nd to midnight at start of Jan 3rd)
        System.out.println("Querying for customer A on January 2nd");
        graph.setTimeWindow(startJan2nd, startJan3rd);
        iter = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
        for (GraphElementWithStatistics gews : iter) {
            System.out.println(gews);
        }
        iter.close();
        System.out.println();

        // Query for customer A over all time
        System.out.println("Querying for customer A over all time - note that counts from different days are summed");
        graph.setTimeWindowToAllTime();
        iter = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
        for (GraphElementWithStatistics gews : iter) {
            System.out.println(gews);
        }
        iter.close();
    }

    private static Set<GraphElementWithStatistics> getData() {
        Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();

        // Edge 1 and some statistics for it
        Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "online", true, "public", startJan1st, startJan2nd);
        SetOfStatistics statistics1 = new SetOfStatistics();
        statistics1.addStatistic("count", new Count(1));
        data.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));

        // Edge 2 and some statistics for it
        Edge edge2 = new Edge("customer", "A", "product", "P", "purchase", "online", true, "public", startJan2nd, startJan3rd);
        SetOfStatistics statistics2 = new SetOfStatistics();
        statistics2.addStatistic("count", new Count(2));
        data.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics2));

        // Edge 3 and some statistics for it
        Edge edge3 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, "public", startJan2nd, startJan3rd);
        SetOfStatistics statistics3 = new SetOfStatistics();
        statistics3.addStatistic("count", new Count(17));
        data.add(new GraphElementWithStatistics(new GraphElement(edge3), statistics3));

        // Edge 4 and some statistics for it
        Edge edge4 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, "public", startJan2nd, startJan3rd);
        SetOfStatistics statistics4 = new SetOfStatistics();
        statistics4.addStatistic("countSomething", new Count(123456));
        data.add(new GraphElementWithStatistics(new GraphElement(edge4), statistics4));

        // Edge 5 and some statistics for it
        Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, "public", startJan2nd, startJan3rd);
        SetOfStatistics statistics5 = new SetOfStatistics();
        statistics5.addStatistic("count", new Count(99));
        data.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

        // Entity 1 and some statistics for it
        Entity entity1 = new Entity("customer", "A", "purchase", "online", "public", startJan1st, startJan2nd);
        SetOfStatistics statisticsEntity1 = new SetOfStatistics();
        statisticsEntity1.addStatistic("count", new Count(1000000));
        data.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));

        // Entity 2 and some statistics for it
        Entity entity2 = new Entity("customer", "B", "purchase", "instore", "public", startJan1st, startJan2nd);
        SetOfStatistics statisticsEntity2 = new SetOfStatistics();
        statisticsEntity2.addStatistic("count", new Count(99));
        data.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));

        // Entity 3 and some statistics for it
        Entity entity3 = new Entity("product", "R", "purchase", "instore", "public", startJan1st, startJan2nd);
        SetOfStatistics statisticsEntity3 = new SetOfStatistics();
        statisticsEntity3.addStatistic("count", new Count(99));
        data.add(new GraphElementWithStatistics(new GraphElement(entity3), statisticsEntity3));

        return data;
    }

}
