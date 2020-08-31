/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.kafka.hook;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.kafka.hook.entities.AtlasQualifiedName;
import org.apache.atlas.kafka.hook.entities.LazyAtlasConfigStore;
import org.apache.atlas.kafka.hook.entities.TopicEntity;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;


public class KafkaHookIT {

    public static final String ATLAS_ENDPOINT = "atlas.rest.address";
    public static final String REFERENCE_ATTRIBUTE = "qualifiedName";
    public static final String KAFKA_TOPIC_ENTITY_TYPE = "kafka_topic";
    public static final String PRODUCER_ENTITY_TYPE = "kafka_producer";
    public static final String CONSUMER_ENTITY_TYPE = "kafka_consumer";
    public static final String CONSUMER_GROUP_ENTITY_TYPE = "kafka_consumer_group";
    public static final String PRODUCER_LINEAGE_ENTITY_TYPE = "kafka_producer_lineage";
    public static final String CONSUMER_LINEAGE_ENTITY_TYPE = "kafka_consumer_lineage";
    public static final String LINEAGE_POSTFIX = "_lineage";
    public static final String METADATA_NAMESPACE = "cm";
    public static final int CALL_TIMEOUT = 15000;
    public static final String RELATIONSHIP_REFERENCE_ATTRIBUTE = "displayText";
    public static final String RELATIONSHIP_OUTPUTS = "outputs";
    public static final String RELATIONSHIP_INPUTS = "inputs";
    private static AtlasClientV2 atlasClientV2;
//    private static EmbeddedKafkaCluster kafka;

    private AdminClient kafkaAdmin;
    private List<AtlasQualifiedName> topicsToCleanUp;
    private List<AtlasQualifiedName> producersToCleanUp;
    private List<AtlasQualifiedName> consumersToCleanUp;
    private List<AtlasQualifiedName> groupsToCleanUp;

    @BeforeMethod
    public void setup() throws Exception{
        Configuration configuration = ApplicationProperties.get();
        String[] atlasEndPoint = configuration.getStringArray(ATLAS_ENDPOINT);

        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            atlasClientV2 = new AtlasClientV2(atlasEndPoint, new String[]{"admin", "admin123"});
        } else {
            atlasClientV2 = new AtlasClientV2(atlasEndPoint);
        }

        Properties kafkaConfig = new Properties();
        kafkaConfig.put("cloudera.auditors", "org.apache.atlas.kafka.hook.KafkaHook");

        //Starting Kafka requires before each test, because of Cache in the Auditor Hook
//        kafka = new EmbeddedKafkaCluster(3, kafkaConfig);
//        kafka.start();

        Properties adminProperties = new Properties();
//        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        kafkaAdmin = KafkaAdminClient.create(adminProperties);

        topicsToCleanUp = new ArrayList<>();
        producersToCleanUp = new ArrayList<>();
        consumersToCleanUp = new ArrayList<>();
        groupsToCleanUp = new ArrayList<>();
    }

    @AfterMethod
    public void cleanUp() throws Exception{
        List<String> topicNames = topicsToCleanUp.stream()
                .map(AtlasQualifiedName::getUniqueIdentifier).collect(Collectors.toList());
        kafkaAdmin.deleteTopics(topicNames).all().get();
        waitForCondition(() -> {
            try {
                return !kafkaAdmin.listTopics().names().get().removeAll(topicNames);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        List<String> topicQualifiedNames = topicsToCleanUp.stream()
                .map(AtlasQualifiedName::getQualifiedName).collect(Collectors.toList());
        cleanUp(KAFKA_TOPIC_ENTITY_TYPE, topicQualifiedNames);

        List<String> producerQualifiedNames = producersToCleanUp.stream()
                .map(AtlasQualifiedName::getQualifiedName).collect(Collectors.toList());
        cleanUp(PRODUCER_ENTITY_TYPE, producerQualifiedNames);

        List<String> consumerQualifiedNames = consumersToCleanUp.stream()
                .map(AtlasQualifiedName::getQualifiedName).collect(Collectors.toList());
        cleanUp(CONSUMER_ENTITY_TYPE, consumerQualifiedNames);

        List<String> groupQualifiedNames = groupsToCleanUp.stream()
                .map(AtlasQualifiedName::getQualifiedName).collect(Collectors.toList());
        cleanUp(CONSUMER_GROUP_ENTITY_TYPE, groupQualifiedNames);

        kafkaAdmin.close();
        atlasClientV2.close();
    }

    @Test
    public void testCreateTopicThroughKafka() throws Exception {
        AtlasQualifiedName topic = registerRandomTopic();

        AtlasEntity entity = waitForEntity(KAFKA_TOPIC_ENTITY_TYPE, topic.getQualifiedName());
        Assert.assertEquals(entity.getAttribute(REFERENCE_ATTRIBUTE), topic.getQualifiedName());
    }

    @Test
    public void testProduce_OneMessage() throws Exception{
        AtlasQualifiedName topic = registerRandomTopic();

        AtlasQualifiedName producerName = registerRandomProducer();
        String producerLineageQualifiedName = producerName.getUniqueIdentifier() + LINEAGE_POSTFIX + "@" + METADATA_NAMESPACE;
        KafkaProducer<String, String> producer = createTestProducer(producerName.getUniqueIdentifier());

        producer.send(new ProducerRecord<>(topic.getUniqueIdentifier(), "key", "value")).get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);

        AtlasEntity entity = waitForEntity(PRODUCER_ENTITY_TYPE, producerName.getQualifiedName());
        Assert.assertEquals(entity.getAttribute(REFERENCE_ATTRIBUTE), producerName.getQualifiedName());

        AtlasEntity lineageEntity = waitForEntity(PRODUCER_LINEAGE_ENTITY_TYPE, producerLineageQualifiedName);
        Assert.assertEquals(lineageEntity.getAttribute(REFERENCE_ATTRIBUTE), producerLineageQualifiedName);

        List<Map<String, String>> kafkaTopics = (List<Map<String, String>>) lineageEntity.getRelationshipAttribute(RELATIONSHIP_OUTPUTS);
        Assert.assertEquals(kafkaTopics.size(), 1);
        Assert.assertEquals(kafkaTopics.get(0).get(RELATIONSHIP_REFERENCE_ATTRIBUTE), topic.getUniqueIdentifier());
    }

    @Test
    public void testProduce_SameProducerMultipleTopics_RelationshipsAppended() throws Exception {
        AtlasQualifiedName topic1 = registerRandomTopic();
        String topic1Name = topic1.getUniqueIdentifier();
        AtlasQualifiedName topic2 = registerRandomTopic();
        String topic2Name = topic2.getUniqueIdentifier();
        AtlasQualifiedName producerName = registerRandomProducer();
        KafkaProducer<String, String> producer = createTestProducer(producerName.getUniqueIdentifier());
        String producerLineageQualifiedName = producerName.getUniqueIdentifier() + LINEAGE_POSTFIX + "@" + METADATA_NAMESPACE;

        producer.send(new ProducerRecord<>(topic1Name, "key", "value")).get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        producer.send(new ProducerRecord<>(topic2Name, "key", "value")).get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);

        AtlasEntity entity = waitForEntity(PRODUCER_ENTITY_TYPE, producerName.getQualifiedName());
        Assert.assertEquals(entity.getAttribute(REFERENCE_ATTRIBUTE), producerName.getQualifiedName());

        AtlasEntity lineageEntity = waitForEntity(PRODUCER_LINEAGE_ENTITY_TYPE, producerLineageQualifiedName);
        Assert.assertEquals(lineageEntity.getAttribute(REFERENCE_ATTRIBUTE), producerLineageQualifiedName);

        List<Map<String, String>> kafkaTopics = (List<Map<String, String>>) lineageEntity.getRelationshipAttribute(RELATIONSHIP_OUTPUTS);
        Assert.assertEquals(kafkaTopics.size(), 2);
        MatcherAssert.assertThat(kafkaTopics.get(0).get(RELATIONSHIP_REFERENCE_ATTRIBUTE),
            Matchers.either(Matchers.equalTo(topic1Name)).or(Matchers.equalTo(topic2Name)));
        MatcherAssert.assertThat(kafkaTopics.get(1).get(RELATIONSHIP_REFERENCE_ATTRIBUTE),
            Matchers.either(Matchers.equalTo(topic1Name)).or(Matchers.equalTo(topic2Name)));
    }

    @Test
    public void testProduce_ProducerConsumerMultipleTopics_RelationshipsAppended() throws Exception{
        AtlasQualifiedName topic1 = registerRandomTopic();
        String topic1Name = topic1.getUniqueIdentifier();
        AtlasQualifiedName topic2 = registerRandomTopic();
        String topic2Name = topic2.getUniqueIdentifier();
        AtlasQualifiedName complexClient = registerRandomProducer();
        String complexClientName = complexClient.getUniqueIdentifier();
        registerConsumer(complexClientName);
        AtlasQualifiedName consumerQn = registerRandomConsumer();
        String consumerName = consumerQn.getUniqueIdentifier();

        KafkaProducer<String, String> producer = createTestProducer(complexClientName);
        KafkaConsumer<String, String> consumer = createTestConsumer(complexClientName);
        KafkaConsumer<String, String> loneConsumer = createTestConsumer(consumerName);

        CountDownLatch producerLatch = new CountDownLatch(2);
        producer.send(new ProducerRecord<>(topic1Name, "key", "value"), (metadata, exception) -> producerLatch.countDown())
                .get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        producer.send(new ProducerRecord<>(topic2Name, "key", "value"), (metadata, exception) -> producerLatch.countDown())
                .get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        consumer.subscribe(Collections.singletonList(topic1Name));
        consumer.poll(Duration.ofMillis(CALL_TIMEOUT));
        loneConsumer.subscribe(Collections.singletonList(topic2Name));
        loneConsumer.poll(Duration.ofMillis(CALL_TIMEOUT));
        Assert.assertTrue(producerLatch.await(CALL_TIMEOUT, TimeUnit.MILLISECONDS));

        AtlasEntity entity = waitForEntity(PRODUCER_ENTITY_TYPE, complexClient.getQualifiedName());
        Assert.assertEquals(entity.getAttribute(REFERENCE_ATTRIBUTE), complexClient.getQualifiedName());

        String complexClientLineageQualifiedName = complexClientName + LINEAGE_POSTFIX + "@" + METADATA_NAMESPACE;
        String loneConsumerLineageQualifiedName = consumerName + LINEAGE_POSTFIX + "@" + METADATA_NAMESPACE;

        AtlasEntity producerLineageEntity = waitForEntity(PRODUCER_LINEAGE_ENTITY_TYPE, complexClientLineageQualifiedName);
        Assert.assertEquals(producerLineageEntity.getAttribute(REFERENCE_ATTRIBUTE), complexClientLineageQualifiedName);

        List<Map<String, String>> kafkaProducerTopics = (List<Map<String, String>>) producerLineageEntity.getRelationshipAttribute(RELATIONSHIP_OUTPUTS);
        Assert.assertEquals(kafkaProducerTopics.size(), 2);
        MatcherAssert.assertThat(kafkaProducerTopics.get(0).get(RELATIONSHIP_REFERENCE_ATTRIBUTE),
                Matchers.either(Matchers.equalTo(topic1Name)).or(Matchers.equalTo(topic2Name)));
        MatcherAssert.assertThat(kafkaProducerTopics.get(1).get(RELATIONSHIP_REFERENCE_ATTRIBUTE),
                Matchers.either(Matchers.equalTo(topic1Name)).or(Matchers.equalTo(topic2Name)));

        AtlasEntity complexConsumerLineageEntity = waitForEntity(CONSUMER_LINEAGE_ENTITY_TYPE, complexClientLineageQualifiedName);
        Assert.assertEquals(complexConsumerLineageEntity.getAttribute(REFERENCE_ATTRIBUTE), complexClientLineageQualifiedName);

        List<Map<String, String>> kafkaComplexConsumerTopics = (List<Map<String, String>>) complexConsumerLineageEntity.getRelationshipAttribute(RELATIONSHIP_INPUTS);

        Assert.assertEquals(kafkaComplexConsumerTopics.size(), 1);
        MatcherAssert.assertThat(kafkaComplexConsumerTopics.get(0).get(RELATIONSHIP_REFERENCE_ATTRIBUTE),
                Matchers.equalTo(topic1Name));


        AtlasEntity loneConsumerLineageEntity = waitForEntity(CONSUMER_LINEAGE_ENTITY_TYPE, loneConsumerLineageQualifiedName);
        Assert.assertEquals(loneConsumerLineageEntity.getAttribute(REFERENCE_ATTRIBUTE), loneConsumerLineageQualifiedName);

        List<Map<String, String>> kafkaLoneConsumerTopics = (List<Map<String, String>>) loneConsumerLineageEntity.getRelationshipAttribute(RELATIONSHIP_INPUTS);

        Assert.assertEquals(kafkaLoneConsumerTopics.size(), 1);
        MatcherAssert.assertThat(kafkaLoneConsumerTopics.get(0).get(RELATIONSHIP_REFERENCE_ATTRIBUTE),
               Matchers.equalTo(topic2Name));
    }

    @Test
    public void testProduce_MultipleMessage_CreatedOnlyOneEntity() throws Exception{
        AtlasQualifiedName topic1 = registerRandomTopic();
        String topic1Name = topic1.getUniqueIdentifier();
        AtlasQualifiedName producerQn = registerRandomProducer();
        String producerName = producerQn.getUniqueIdentifier();

        KafkaProducer<String, String> producer = createTestProducer(producerName);

        producer.send(new ProducerRecord<>(topic1Name, "key1", "value1")).get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        producer.send(new ProducerRecord<>(topic1Name, "key2", "value2")).get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        producer.send(new ProducerRecord<>(topic1Name, "key3", "value3")).get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);

        waitForEntity(PRODUCER_ENTITY_TYPE, producerQn.getQualifiedName());
        List<AtlasEntity> entities = atlasClientV2.getEntitiesByAttribute(PRODUCER_ENTITY_TYPE,
            Collections.singletonList(Collections.singletonMap(REFERENCE_ATTRIBUTE, producerQn.getQualifiedName()))).getEntities();
        Assert.assertNotNull(entities);
        Assert.assertEquals(entities.size(), 1);
        AtlasEntity entity = entities.get(0);
        Assert.assertEquals(entity.getAttribute(REFERENCE_ATTRIBUTE), producerQn.getQualifiedName());

        String producerLineageQualifiedName = producerName + LINEAGE_POSTFIX + "@" + METADATA_NAMESPACE;

        AtlasEntity producerLineageEntity = waitForEntity(PRODUCER_LINEAGE_ENTITY_TYPE, producerLineageQualifiedName);
        Assert.assertEquals(producerLineageEntity.getAttribute(REFERENCE_ATTRIBUTE), producerLineageQualifiedName);

        List<Map<String, String>> kafkaProducerTopics = (List<Map<String, String>>) producerLineageEntity.getRelationshipAttribute(RELATIONSHIP_OUTPUTS);
        Assert.assertEquals(kafkaProducerTopics.size(), 1);
        MatcherAssert.assertThat(kafkaProducerTopics.get(0).get(RELATIONSHIP_REFERENCE_ATTRIBUTE),
                Matchers.either(Matchers.equalTo(topic1Name)).or(Matchers.equalTo(topic1Name)));
    }

    @Test
    public void testConsume() throws Exception {
        AtlasQualifiedName topic1 = registerRandomTopic();
        String topic1Name = topic1.getUniqueIdentifier();
        AtlasQualifiedName consumerQn = registerRandomConsumer();
        KafkaConsumer<String, String> consumer = createTestConsumer(consumerQn.getUniqueIdentifier());

        consumer.subscribe(Collections.singletonList(topic1Name));
        consumer.poll(Duration.ofMillis(CALL_TIMEOUT));

        AtlasEntity entity = waitForEntity(CONSUMER_ENTITY_TYPE, consumerQn.getQualifiedName());
        Assert.assertEquals(entity.getAttribute(REFERENCE_ATTRIBUTE), consumerQn.getQualifiedName());

        String consumerLineageQualifiedName = consumerQn.getUniqueIdentifier() + LINEAGE_POSTFIX + "@" + METADATA_NAMESPACE;

        AtlasEntity consumerLineageEntity = waitForEntity(CONSUMER_LINEAGE_ENTITY_TYPE, consumerLineageQualifiedName);
        Assert.assertEquals(consumerLineageEntity.getAttribute(REFERENCE_ATTRIBUTE), consumerLineageQualifiedName);

        List<Map<String, String>> kafkaConsumerTopics = (List<Map<String, String>>) consumerLineageEntity.getRelationshipAttribute(RELATIONSHIP_INPUTS);

        Assert.assertEquals(kafkaConsumerTopics.size(), 1);
        MatcherAssert.assertThat(kafkaConsumerTopics.get(0).get(RELATIONSHIP_REFERENCE_ATTRIBUTE),
                Matchers.equalTo(topic1Name));
    }

    @Test
    public void testConsume_MultipleConsumerGroupsSameId_AllGroupIdsStored() throws Exception {
        AtlasQualifiedName topic1 = registerRandomTopic();
        String topic1Name = topic1.getUniqueIdentifier();
        AtlasQualifiedName topic2 = registerRandomTopic();
        String topic2Name = topic2.getUniqueIdentifier();
        AtlasQualifiedName producerQn = registerRandomProducer();
        AtlasQualifiedName consumer1Qn = registerRandomConsumer();
        AtlasQualifiedName consumer2Qn = registerRandomConsumer();
        AtlasQualifiedName group1Qn = registerRandomGroup();
        String group1Name = group1Qn.getUniqueIdentifier();
        AtlasQualifiedName group2Qn = registerRandomGroup();
        String group2Name = group2Qn.getUniqueIdentifier();

        KafkaProducer<String, String> producer = createTestProducer(producerQn.getUniqueIdentifier());
        KafkaConsumer<String, String> consumer1a = createTestConsumer(consumer1Qn.getUniqueIdentifier(), group1Name);
        KafkaConsumer<String, String> consumer1b = createTestConsumer(consumer1Qn.getUniqueIdentifier(), group2Name);
        KafkaConsumer<String, String> consumer2 = createTestConsumer(consumer2Qn.getUniqueIdentifier(), group1Name);

        producer.send(new ProducerRecord<>(topic1Name, "key1", "value1")).get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        consumer1a.subscribe(Collections.singletonList(topic1Name));
        consumer1a.poll(Duration.ofMillis(CALL_TIMEOUT));
        consumer1a.close();

        producer.send(new ProducerRecord<>(topic1Name, "key1", "value1")).get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        consumer1b.subscribe(Collections.singletonList(topic1Name));
        consumer1b.poll(Duration.ofMillis(CALL_TIMEOUT));
        consumer1b.close();

        producer.send(new ProducerRecord<>(topic2Name, "key3", "value3")).get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        consumer2.subscribe(Collections.singletonList(topic2Name));
        consumer2.poll(Duration.ofMillis(CALL_TIMEOUT));
        consumer2.close();

        waitForCondition(() -> {
            AtlasEntity consumerEntity1 = waitForEntity(CONSUMER_ENTITY_TYPE, consumer1Qn.getQualifiedName());
            Assert.assertEquals(consumerEntity1.getAttribute(REFERENCE_ATTRIBUTE), consumer1Qn.getQualifiedName());
            List<Object> groupRelationships = (List) consumerEntity1.getRelationshipAttribute("kafkaConsumerGroups");
            // assert only if we have both groups
            if (2 == groupRelationships.size()) {
                MatcherAssert.assertThat((String) ((HashMap) groupRelationships.get(0)).get("displayText"),
                        Matchers.either(Matchers.equalTo(group1Name)).or(Matchers.equalTo(group2Name)));
                MatcherAssert.assertThat((String) ((HashMap) groupRelationships.get(1)).get("displayText"),
                        Matchers.either(Matchers.equalTo(group1Name)).or(Matchers.equalTo(group2Name)));
            }
            // otherwise wait
            return 2 == groupRelationships.size();
        });

        AtlasEntity consumerEntity2 = waitForEntity(CONSUMER_ENTITY_TYPE, consumer2Qn.getQualifiedName());
        Assert.assertEquals(consumerEntity2.getAttribute(REFERENCE_ATTRIBUTE), consumer2Qn.getQualifiedName());
        List<Object> groupRelationships2 = (List) consumerEntity2.getRelationshipAttribute("kafkaConsumerGroups");
        Assert.assertEquals(1, groupRelationships2.size());
        Assert.assertEquals(((HashMap) groupRelationships2.get(0)).get("displayText"), group1Name);
    }

    @Test
    public void testCreatePartition() throws Exception {
        AtlasQualifiedName topic1 = registerRandomTopic();
        String topic1Name = topic1.getUniqueIdentifier();

        AtlasEntity entity = waitForEntity(KAFKA_TOPIC_ENTITY_TYPE, topic1.getQualifiedName());
        Assert.assertEquals(1, entity.getAttribute("partitionCount"));

        Map<String, NewPartitions> newPartitions = new HashMap<>();
        newPartitions.put(topic1Name, NewPartitions.increaseTo(10));
        kafkaAdmin.createPartitions(newPartitions).all().get();
        Thread.sleep(CALL_TIMEOUT);
        waitForCondition(() -> {
            try {
                return kafkaAdmin.describeTopics(Collections.singletonList(topic1Name))
                        .all().get().get(topic1Name).partitions().size() == 10;
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        waitForCondition(() -> {
            AtlasEntity modifiedEntity = waitForEntity(KAFKA_TOPIC_ENTITY_TYPE, topic1.getQualifiedName());
            return (int)modifiedEntity.getAttribute("partitionCount") == 10;
        });
    }

    @Test
    public void testUpdateTopicReplicationFactor() throws Exception {
        AtlasQualifiedName topicQn = randomId("topic" + topicsToCleanUp.size());
        String topicName = topicQn.getUniqueIdentifier();
        topicsToCleanUp.add(topicQn);

        kafkaAdmin.createTopics(ImmutableList.of(
            new NewTopic(topicName, ImmutableMap.of(0, Collections.singletonList(0)))
        )).all().get();
        waitForTopic(topicName);

        kafkaAdmin.alterPartitionReassignments(ImmutableMap.of(
            new TopicPartition(topicName, 0),
            Optional.of(new NewPartitionReassignment(ImmutableList.of(0, 1)))
        )).all().get();
        waitForCondition(() -> {
            try {
                return kafkaAdmin.describeTopics(Collections.singletonList(topicName))
                        .all().get().get(topicName).partitions().get(0).replicas().size() == 2;
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        AtlasEntity entity = waitForEntity(KAFKA_TOPIC_ENTITY_TYPE, topicQn.getQualifiedName());
        Assert.assertEquals(2, entity.getAttribute(TopicEntity.REPLICATION_FACTOR));
    }

    public KafkaConsumer<String, String> createTestConsumer(String consumerName) {
        String group = registerRandomGroup().getUniqueIdentifier();
        return createTestConsumer(consumerName, group);
    }

    public KafkaConsumer<String, String> createTestConsumer(String consumerName, String groupId) {
        Properties props = new Properties();
//        String kafkaBootstrapServers = kafka.bootstrapServers();
//        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, consumerName);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }


    public KafkaProducer<String, String> createTestProducer(String producerName) {
        Properties props = new Properties();
//        String kafkaBootstrapServers = kafka.bootstrapServers();
//        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerName);

        return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    private void cleanUp(String typeName, List<String> entities) throws Exception {
        for (String entity : entities) {
            atlasClientV2.deleteEntityByAttribute(typeName, Collections.singletonMap(REFERENCE_ATTRIBUTE, entity));
        }
    }

    private void waitForTopic(String topicName) {
        waitForCondition(() -> {
            try {
                return kafkaAdmin.listTopics().names().get().contains(topicName);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void waitForCondition(BooleanSupplier condition) {
        long startTime = System.currentTimeMillis();
        boolean conditionResult;
        while (!(conditionResult = condition.getAsBoolean()) && System.currentTimeMillis() - startTime < CALL_TIMEOUT) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Assert.fail("Waiting for condition interrupted");
            }
        }
        if (System.currentTimeMillis() - startTime >= CALL_TIMEOUT && !conditionResult) {
            Assert.fail("Timeout expired and condition wasn't satisfied");
        }
    }

    private AtlasEntity waitForEntity(String entityType, String uniqueAttributeValue) {
        long startTime = System.currentTimeMillis();
        AtlasEntity entity;
        while ((entity = getEntityByUniqueAttributes(entityType, uniqueAttributeValue)) == null
                && System.currentTimeMillis() - startTime < CALL_TIMEOUT) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Assert.fail("Waiting for condition interrupted");
            }
        }
        if (System.currentTimeMillis() - startTime >= CALL_TIMEOUT && entity == null) {
            Assert.fail(String.format("Timeout expired and entity wasn't created until then: %s, {%s: %s}",
                    entityType, REFERENCE_ATTRIBUTE, uniqueAttributeValue));
        }
        return entity;
    }

    private AtlasEntity getEntityByUniqueAttributes(String entityType, String uniqueAttributeValue) {
        try {
            return atlasClientV2.getEntityByAttribute(entityType,
                    Collections.singletonMap(REFERENCE_ATTRIBUTE, uniqueAttributeValue)).getEntity();
        } catch (AtlasServiceException e) {
            if (e.getMessage().contains(AtlasErrorCode.INSTANCE_NOT_FOUND.getErrorCode())) {
                return null;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private AtlasQualifiedName registerRandomTopic() throws InterruptedException, ExecutionException {
        AtlasQualifiedName topicName = randomId("topic" + topicsToCleanUp.size());
        topicsToCleanUp.add(topicName);
        kafkaAdmin.createTopics(Collections.singletonList(new NewTopic(topicName.getUniqueIdentifier(), 1, (short)1)))
            .all().get();
        waitForTopic(topicName.getUniqueIdentifier());
        return topicName;
    }

    private AtlasQualifiedName registerRandomProducer() {
        AtlasQualifiedName producerName = randomId("producer" + producersToCleanUp.size());
        producersToCleanUp.add(producerName);
        return producerName;
    }

    private AtlasQualifiedName registerConsumer(String name) {
        AtlasQualifiedName consumerName = AtlasQualifiedName.of(name, LazyAtlasConfigStore.getInstance().getAtlasMetadataNamespace());
        consumersToCleanUp.add(consumerName);
        return consumerName;
    }

    private AtlasQualifiedName registerRandomConsumer() {
        AtlasQualifiedName consumerName = randomId("consumer" + consumersToCleanUp.size());
        consumersToCleanUp.add(consumerName);
        return consumerName;
    }

    private AtlasQualifiedName registerRandomGroup() {
        AtlasQualifiedName groupName = randomId("group" + groupsToCleanUp.size());
        groupsToCleanUp.add(groupName);
        return groupName;
    }

    private static AtlasQualifiedName randomId(String prefix) {
        return AtlasQualifiedName.of(prefix + "-" + UUID.randomUUID().toString(), LazyAtlasConfigStore.getInstance().getAtlasMetadataNamespace());
    }
}
