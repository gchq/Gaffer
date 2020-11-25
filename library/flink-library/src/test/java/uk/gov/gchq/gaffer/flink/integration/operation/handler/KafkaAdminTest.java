package uk.gov.gchq.gaffer.flink.integration.operation.handler;

import kafka.admin.AdminClient;
import kafka.admin.AdminUtils;
import kafka.admin.ConsumerGroupCommand;
import kafka.admin.RackAwareMode;
import kafka.common.TopicAndPartition;
import kafka.coordinator.GroupOverview;
import kafka.coordinator.GroupSummary;
import kafka.coordinator.MemberSummary;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import org.apache.curator.test.TestingServer;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;
import scala.Option;
import scala.collection.JavaConversions;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class KafkaAdminTest {

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    @Rule
    public final RetryRule rule = new RetryRule();

    private KafkaServer kafkaServer;
    private TestingServer zkServer;
    private String bootstrapServers;

    @BeforeEach
    public void before() throws Exception {
        bootstrapServers = "localhost:" + getOpenPort();

        // Create zookeeper server
        zkServer = new TestingServer(-1, createZookeeperTmpDir());
        zkServer.start();

        System.out.println("zookeeper " + zkServer.getConnectString());

        kafkaServer = new KafkaServer(new KafkaConfig(serverProperties()), new MockTime(), Option.empty());
        kafkaServer.startup();
        // Create kafka server
        //kafkaServer = TestUtils.createServer(new KafkaConfig(serverProperties()), new MockTime());
        System.out.println(kafkaServer.logIdent());
    }

    @AfterEach
    public void cleanUp() throws IOException {

        if (null != kafkaServer) {
            kafkaServer.shutdown();
        }

        if (null != zkServer) {
            zkServer.close();
        }
    }

    @Test
    public void testAdminClient() throws Exception {

        final String topic = UUID.randomUUID().toString();

        System.out.println("topic : " + topic);

        AdminUtils.createTopic(kafkaServer.zkUtils(), topic, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

        System.out.println("AdminUtils client id " + AdminUtils.AdminClientId());

        System.out.println("Topic exists? " + AdminUtils.topicExists(kafkaServer.zkUtils(), topic));


        createConsumer(topic);

        createProducer(topic);

        Thread.sleep(1000L);





        final AdminClient adminClient = createAdminClient();


        scala.collection.immutable.List<AdminClient.ConsumerSummary> consumerSummary = adminClient.describeConsumerGroup("anothergroup");
        if (consumerSummary.isEmpty()) {
            System.out.println("No Consumers in anothergroup");
        } else {
            JavaConversions.seqAsJavaList(consumerSummary).stream().forEach(cs -> {
                System.out.println("ConsumerSummary: ClientId: " + cs.clientId() + " ClientHost: " + cs.clientHost() + " MemberId: " + cs.memberId());

                final List<TopicAndPartition> topicPartitions = JavaConversions.seqAsJavaList(cs.assignment())
                        .stream()
                        .map(tp -> new TopicAndPartition(tp.topic(), tp.partition()))
                        .collect(Collectors.toList());

                final KafkaConsumer consumerX = createConsumer();

                topicPartitions.forEach(tp -> {
                    final OffsetAndMetadata oAndM = consumerX.committed(new TopicPartition(tp.topic(), tp.partition()));
                    System.out.println(oAndM.offset());
                });
//
//                val partitionOffsets = topicPartitions.flatMap { topicPartition =>
//                    Option(consumer.committed(new TopicPartition(topicPartition.topic, topicPartition.partition))).map { offsetAndMetadata =>
//                        topicPartition -> offsetAndMetadata.offset
//                    }
//                }.toMap
//                describeTopicPartition(group, topicPartitions, partitionOffsets.get,
//                        _ => Some(s"${consumerSummary.clientId}_${consumerSummary.clientHost}"))

            });
        }
        System.out.println(">..........................................................");

        scala.collection.immutable.List<GroupOverview> consumerGroups = adminClient.listAllConsumerGroupsFlattened();
        if (consumerGroups.isEmpty()) {
            System.out.println("No consumer Groups");
        } else {
            JavaConversions.seqAsJavaList(consumerGroups)
                    .stream()
                    .peek(System.out::println)
                    .count();
        }


        runCommand();
//
//        System.out.println(adminClient.logIdent());
//
//
//
//        scala.collection.immutable.List<Node> brokers = adminClient.bootstrapBrokers();
//        if (brokers.isEmpty()) {
//            System.out.println("No brokers....");
//        } else {
//            JavaConversions.seqAsJavaList(brokers).stream().forEach(n -> {
//                System.out.println(n.toString());
//                scala.collection.immutable.List<GroupOverview> gov = adminClient.listGroups(n);
//                if (gov.isEmpty()) {
//                    System.out.println("No groups for node: " + n);
//                } else {
//                    JavaConversions.seqAsJavaList(gov).stream().forEach(x -> System.out.println(x));
//                }
//
//            });
//        }
//
//
//        scala.collection.immutable.List<GroupOverview> consumerGroups = adminClient.listAllConsumerGroupsFlattened();
//        if (consumerGroups.isEmpty()) {
//            System.out.println("No consumer Groups");
//        }
//        JavaConversions.seqAsJavaList(adminClient.listAllConsumerGroupsFlattened())
//                .stream()
//                .map(GroupOverview::groupId)
//                .peek(System.out::println)
//                .count();
//
//        scala.collection.immutable.List<GroupOverview> groups = adminClient.listAllGroupsFlattened();
//        if (groups.isEmpty()) {
//            System.out.println("No Groups");
//        }
//        JavaConversions.seqAsJavaList(adminClient.listAllGroupsFlattened())
//                .stream()
//                .map(GroupOverview::groupId)
//                .peek(System.out::println)
//                .count();


        //JavaConversions.seqAsJavaList(adminClient.describeConsumerGroup("groupId")).forEach(c -> System.out.println(c));


        //System.out.println("Pending request count : " + adminClient.client().pendingRequestCount());

    }

    private KafkaConsumer createConsumer(final String topic) {
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(asList(topic));
        consumer.poll(10L);
        return consumer;
    }

    private KafkaConsumer createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "anothergroup");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("security.protocol", "PLAINTEXT");
        return new KafkaConsumer<String, String>(props);
    }

    private KafkaProducer createProducer(final String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "anothergroup");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol", "PLAINTEXT");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        return producer;
    }

    private File createZookeeperTmpDir() throws IOException {
        testFolder.delete();
        testFolder.create();
        return testFolder.newFolder("zkTmpDir");
    }

    private Properties producerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    private Properties serverProperties() {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkServer.getConnectString());
        props.put("broker.id", "99");
        props.put("offsets.topic.replication.factor", "1");
        props.setProperty("listeners", "PLAINTEXT://" + bootstrapServers);
        //props.setProperty("advertisedHostName", "127.0.0.1");
        return props;
    }

    private static int getOpenPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            int port = socket.getLocalPort();
            System.out.println("port: " + port);
            return port;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private AdminClient createAdminClient() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("broker.id", "99");
        props.put("security.protocol", "PLAINTEXT");
        props.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
        return AdminClient.create(props);
    }

    private void runCommand() {
        final String[] args = new String[8];
        args[0] = "--bootstrap-server";
        args[1] = bootstrapServers;
        args[2] = "--describe";
        args[3] = "--new-consumer";
        args[4] = "--command-config";
        args[5] = "/tmp/command-config.txt";
        args[6] = "--group";
        args[7] = "anothergroup";

//        final String[] args = new String[3];
//        args[0] = "--list";
//        args[1] = "--zookeeper";
//        args[2] = zkServer.getConnectString();
        ConsumerGroupCommand.ConsumerGroupCommandOptions checkArgs = new ConsumerGroupCommand.ConsumerGroupCommandOptions(args);
        checkArgs.checkArgs();
        new ConsumerGroupCommand.KafkaConsumerGroupService(checkArgs).printDescribeHeader();
        new ConsumerGroupCommand.KafkaConsumerGroupService(checkArgs).describeGroup("anothergroup");
    }
}
