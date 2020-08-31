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

import org.apache.atlas.kafka.hook.entities.*;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.server.auditor.AuditInfo;
import org.apache.kafka.server.auditor.Auditor;
import org.apache.kafka.server.auditor.events.ProduceEvent;
import org.apache.kafka.server.auditor.events.SyncGroupEvent;
import org.apache.kafka.server.auditor.events.TopicEvent;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class KafkaHookImplTest {

    private static final String LINEAGE_POSTFIX = "_lineage";

    private AtlasConnector connector;
    private ClientRelationshipCache<ProducerEntity, TopicEntity> producerCache;
    private ClientRelationshipCache<ConsumerEntity, TopicEntity> consumerCache;
    private ClientRelationshipCache<ConsumerEntity, ConsumerGroupEntity> consumerGroupCache;
    private KafkaHookImpl kafkaHook;

    @BeforeMethod
    public void setUp() {
        connector = mock(AtlasConnector.class);
        producerCache = mock(ClientRelationshipCache.class);
        consumerCache = mock(ClientRelationshipCache.class);
        consumerGroupCache = mock(ClientRelationshipCache.class);
        kafkaHook = new KafkaHookImpl(connector, producerCache, consumerCache, consumerGroupCache);
    }

    @Captor
    ArgumentCaptor<List<TopicEntity>> captor;

    @BeforeMethod
    public void init(){
        MockitoAnnotations.openMocks(this);
    }


    @Test
    public void createProducer_relationshipInCache_AtlasClientNotCalled() {
        String clientId = "kafkaProducer";
        String testTopic = "topic1";
        ProducerEntity producerEntity = new ProducerEntity(clientId);
        AuthorizableRequestContext mockContext = mock(AuthorizableRequestContext.class);

//        TopicEvent event = TopicEvent.topicCreateEvent(Collections.singletonMap(
//                new TopicEvent.AuditedTopic(testTopic, 1, 1),
//                new AuditInfo(AclOperation.CREATE, new ResourcePattern(ResourceType.TOPIC, testTopic, PatternType.LITERAL))
//        ));

        ProduceEvent produceEvent = new ProduceEvent(Collections.singletonMap(
                new TopicPartition(testTopic, 1),
                new AuditInfo(AclOperation.CREATE, new ResourcePattern(ResourceType.TOPIC, testTopic, PatternType.LITERAL))
        ), clientId);

        when(producerCache.clientExists(eq(producerEntity))).thenReturn(true);
        when(mockContext.clientId()).thenReturn(clientId);

        kafkaHook.handleProduce(produceEvent, mockContext);

        verifyNoInteractions(connector);
    }

    @Test
    public void createProducer_relationshipAndClientNotInCache_AtlasClientCreateProducerCalled() {
        String clientId = "kafkaProducer";
        String testTopic = "topic1";
        TopicEntity testTopicEntity = new TopicEntity(testTopic);
        ProducerEntity producerEntity = new ProducerEntity(clientId);
        ClientLineageEntity producerLineageEntity = producerEntity.createClientLineageEntity(clientId + LINEAGE_POSTFIX,
                Collections.singletonList(testTopicEntity.toAtlasEntity()));
        AuthorizableRequestContext mockContext = mock(AuthorizableRequestContext.class);
        ProduceEvent produceEvent = new ProduceEvent(Collections.singletonMap(
                new TopicPartition(testTopic, 1),
                new AuditInfo(AclOperation.CREATE, new ResourcePattern(ResourceType.TOPIC, testTopic, PatternType.LITERAL))
        ), clientId);

        when(producerCache.clientExists(eq(producerEntity))).thenReturn(false);
        when(producerCache.putRelationship(producerEntity, testTopicEntity))
                .thenReturn(Optional.of(new ClientRelationship<>(producerEntity, testTopicEntity)));
        when(mockContext.clientId()).thenReturn(clientId);

        kafkaHook.handleProduce(produceEvent, mockContext);

        verify(connector).create(eq(producerEntity));
        verify(connector).create(eq(producerLineageEntity));
    }

    @Test
    public void createProducer_onlyClientInCache_AtlasClientUpdateProducerCalled() {
        String clientId = "kafkaProducer";
        String testTopic = "topic1";
        TopicEntity testTopicEntity = new TopicEntity(testTopic);
        ProducerEntity producerEntity = new ProducerEntity(clientId);
        ClientLineageEntity producerLineageEntity = producerEntity.createClientLineageEntity(clientId + LINEAGE_POSTFIX,
                Collections.singletonList(testTopicEntity.toAtlasEntity()));
        AuthorizableRequestContext mockContext = mock(AuthorizableRequestContext.class);
        ProduceEvent produceEvent = new ProduceEvent(Collections.singletonMap(
                new TopicPartition(testTopic, 1),
                new AuditInfo(AclOperation.CREATE, new ResourcePattern(ResourceType.TOPIC, testTopic, PatternType.LITERAL))
        ), clientId);


        when(producerCache.clientExists(eq(producerEntity))).thenReturn(true);
        when(producerCache.putRelationship(producerEntity, testTopicEntity))
                .thenReturn(Optional.of(new ClientRelationship<>(producerEntity, testTopicEntity)));
        when(mockContext.clientId()).thenReturn(clientId);

        kafkaHook.handleProduce(produceEvent, mockContext);

        verify(connector).update(eq(producerEntity));
        verify(connector).update(eq(producerLineageEntity));
    }

    @Test
    public void createConsumer_relationshipInCache_AtlasClientNotCalled() {
        String clientId = "kafkaConsumer";
        String testGroupId = "groupId";
        String testTopic = "topic1";
        ConsumerEntity consumerEntity = new ConsumerEntity(clientId);
        AuthorizableRequestContext mockContext = mock(AuthorizableRequestContext.class);
        SyncGroupEvent event = new SyncGroupEvent(Collections.singleton(testTopic), clientId, testGroupId,
                new AuditInfo(AclOperation.READ, new ResourcePattern(ResourceType.GROUP, testGroupId, PatternType.LITERAL)));

        when(consumerCache.clientExists(eq(consumerEntity))).thenReturn(true);
        when(mockContext.clientId()).thenReturn(clientId);

        kafkaHook.handleConsume(event, mockContext);

        verifyNoInteractions(connector);
    }

    @Test
    public void createConsumer_relationshipAndClientNotInCache_AtlasClientCreateConsumerCalled() {
        String clientId = "kafkaConsumer";
        String testGroupId = "groupId";
        String testTopic = "topic1";
        TopicEntity testTopicEntity = new TopicEntity(testTopic);
        ConsumerGroupEntity consumerGroupEntity = new ConsumerGroupEntity(testGroupId);
        ConsumerEntity consumerEntity = new ConsumerEntity(clientId);
        consumerEntity.setGroupRelationship(consumerGroupEntity.toAtlasEntity());
        ClientLineageEntity consumerLineageEntity = consumerEntity.createClientLineageEntity(clientId + LINEAGE_POSTFIX,
                Collections.singletonList(testTopicEntity.toAtlasEntity()));
        AuthorizableRequestContext mockContext = mock(AuthorizableRequestContext.class);
        SyncGroupEvent event = new SyncGroupEvent(Collections.singleton(testTopic), clientId, testGroupId,
                new AuditInfo(AclOperation.READ, new ResourcePattern(ResourceType.GROUP, testGroupId, PatternType.LITERAL)));

        when(consumerCache.clientExists(eq(consumerEntity))).thenReturn(false);
        when(consumerCache.putRelationship(consumerEntity, testTopicEntity))
                .thenReturn(Optional.of(new ClientRelationship<>(consumerEntity, testTopicEntity)));
        when(mockContext.clientId()).thenReturn(clientId);

        kafkaHook.handleConsume(event, mockContext);

        verify(connector).create(eq(consumerEntity));
        verify(connector).create(eq(consumerLineageEntity));
    }

    @Test
    public void createConsumer_onlyClientInCache_AtlasClientUpdateConsumerCalled() {
        String clientId = "kafkaConsumer";
        String testGroupId = "groupId";
        String testTopic = "topic1";
        TopicEntity testTopicEntity = new TopicEntity(testTopic);
        ConsumerGroupEntity consumerGroupEntity = new ConsumerGroupEntity(testGroupId);
        ConsumerEntity consumerEntity = new ConsumerEntity(clientId);
        consumerEntity.setGroupRelationship(consumerGroupEntity.toAtlasEntity());
        ClientLineageEntity consumerLineageEntity = consumerEntity.createClientLineageEntity(clientId + LINEAGE_POSTFIX,
                Collections.singletonList(testTopicEntity.toAtlasEntity()));
        AuthorizableRequestContext mockContext = mock(AuthorizableRequestContext.class);
        SyncGroupEvent event = new SyncGroupEvent(Collections.singleton(testTopic), clientId, testGroupId,
                new AuditInfo(AclOperation.READ, new ResourcePattern(ResourceType.GROUP, testGroupId, PatternType.LITERAL)));

        when(consumerCache.clientExists(eq(consumerEntity))).thenReturn(true);
        when(consumerCache.putRelationship(consumerEntity, testTopicEntity))
                .thenReturn(Optional.of(new ClientRelationship<>(consumerEntity, testTopicEntity)));
        when(mockContext.clientId()).thenReturn(clientId);

        kafkaHook.handleConsume(event, mockContext);

        verify(connector).update(eq(consumerEntity));
        verify(connector).update(eq(consumerLineageEntity));
    }

    @Test
    public void createConsumer_missingRelationshipsAtlasThrowsException_ExceptionIsSwallowedRelationshipInvalidatedInCache() {
        String clientId = "kafkaConsumer";
        String testGroupId = "groupId";
        String testTopic = "topic1";
        TopicEntity testTopicEntity = new TopicEntity(testTopic);
        ConsumerEntity consumerEntity = new ConsumerEntity(clientId);
        AuthorizableRequestContext mockContext = mock(AuthorizableRequestContext.class);
        SyncGroupEvent event = new SyncGroupEvent(Collections.singleton(testTopic), clientId, testGroupId,
                new AuditInfo(AclOperation.READ, new ResourcePattern(ResourceType.GROUP, testGroupId, PatternType.LITERAL)));

        when(consumerCache.clientExists(eq(consumerEntity))).thenReturn(true);
        when(consumerCache.putRelationship(consumerEntity, testTopicEntity))
                .thenReturn(Optional.of(new ClientRelationship<>(consumerEntity, testTopicEntity)));
        when(mockContext.clientId()).thenReturn(clientId);
        doThrow(new RuntimeException()).when(connector).update((ConsumerEntity) any());

        kafkaHook.handleConsume(event, mockContext);

        verify(consumerCache).invalidateAll(any());
    }

    @Test
    public void createConsumer_groupNotInCache_AtlasClientCreateConsumerGroupCalled() {
        String clientId = "kafkaConsumer";
        String testGroupId = "groupId";
        String testTopic = "topic1";
        TopicEntity testTopicEntity = new TopicEntity(testTopic);
        ConsumerGroupEntity consumerGroupEntity = new ConsumerGroupEntity(testGroupId);
        ConsumerEntity consumerEntity = new ConsumerEntity(clientId);
        consumerEntity.setGroupRelationship(consumerGroupEntity.toAtlasEntity());
        ClientLineageEntity consumerLineageEntity = consumerEntity.createClientLineageEntity(clientId + LINEAGE_POSTFIX,
                Collections.singletonList(testTopicEntity.toAtlasEntity()));
        AuthorizableRequestContext mockContext = mock(AuthorizableRequestContext.class);
        SyncGroupEvent event = new SyncGroupEvent(Collections.singleton(testTopic), clientId, testGroupId,
                new AuditInfo(AclOperation.READ, new ResourcePattern(ResourceType.GROUP, testGroupId, PatternType.LITERAL)));

        when(consumerGroupCache.putRelationship(consumerEntity, consumerGroupEntity))
                .thenReturn(Optional.of(new ClientRelationship<>(consumerEntity, consumerGroupEntity)));
        when(consumerCache.clientExists(eq(consumerEntity))).thenReturn(true);
        when(consumerCache.putRelationship(consumerEntity, testTopicEntity))
                .thenReturn(Optional.of(new ClientRelationship<>(consumerEntity, testTopicEntity)));
        when(mockContext.clientId()).thenReturn(clientId);

        kafkaHook.handleConsume(event, mockContext);

        verify(connector).create(eq(consumerGroupEntity));
        verify(connector).update(eq(consumerEntity));
        verify(connector).update(eq(consumerLineageEntity));
    }

    @Test
    public void createConsumer_missingGroupRelationshipsAtlasThrowsException_ExceptionIsSwallowedRelationshipInvalidatedInCache() {
        String clientId = "kafkaConsumer";
        String testGroupId = "groupId";
        String testTopic = "topic1";
        TopicEntity testTopicEntity = new TopicEntity(testTopic);
        ConsumerGroupEntity consumerGroupEntity = new ConsumerGroupEntity(testGroupId);
        ConsumerEntity consumerEntity = new ConsumerEntity(clientId);
        consumerEntity.setGroupRelationship(consumerGroupEntity.toAtlasEntity());
        AuthorizableRequestContext mockContext = mock(AuthorizableRequestContext.class);
        SyncGroupEvent event = new SyncGroupEvent(Collections.singleton(testTopic), clientId, testGroupId,
                new AuditInfo(AclOperation.READ, new ResourcePattern(ResourceType.GROUP, testGroupId, PatternType.LITERAL)));

        when(consumerGroupCache.putRelationship(consumerEntity, consumerGroupEntity))
                .thenReturn(Optional.of(new ClientRelationship<>(consumerEntity, consumerGroupEntity)));
        doThrow(new RuntimeException()).when(connector).create((ConsumerGroupEntity) any());
        when(consumerCache.clientExists(eq(consumerEntity))).thenReturn(true);
        when(consumerCache.putRelationship(consumerEntity, testTopicEntity))
                .thenReturn(Optional.of(new ClientRelationship<>(consumerEntity, testTopicEntity)));
        when(mockContext.clientId()).thenReturn(clientId);
        doThrow(new RuntimeException()).when(connector).update((ConsumerEntity) any());

        kafkaHook.handleConsume(event, mockContext);

        verify(consumerGroupCache).invalidateAll(any());
        verify(consumerCache).invalidateAll(any());
    }
//
//    @Test
//    public void testUpdateReplicationFactor() {
//        String topicName = "test";
//        Map<AclOperation, Set<AuditInformation>> auditInfo = Collections.singletonMap(
//            AclOperation.ALTER,
//            Collections.singleton(
//                new AuditInformation(
//                    new ResourcePattern(ResourceType.TOPIC, topicName + "#" + 0,
//                        PatternType.LITERAL),
//                    0
//                )
//            )
//        );
//
//        List<TopicEntity> entities = Collections.singletonList(TopicEntity.withReplicationFactor(topicName, 3));
//        AtlasEntity expectedEntity = entities.get(0).toAtlasEntity();
//        TopicEvent event = new TopicEvent(
//                Collections.singleton(TopicEvent.AuditedTopic.withReplicationFactor(topicName, 3)),
//                TopicEvent.EventType.PARTITION_CHANGE);
//
//
//        when(entitySelector.selectTopicsToUpdateReplicationFactor(any(), any())).thenReturn(entities);
//
//        kafkaHook.handleReplicationFactorChange(event, auditInfo);
//
//        verify(entitySelector).selectTopicsToUpdateReplicationFactor(same(event), same(auditInfo.get(AclOperation.ALTER)));
//
//        ArgumentCaptor<List<TopicEntity>> entityCaptor = ArgumentCaptor.forClass(List.class);
//        verify(connector).update(entityCaptor.capture());
//
//        List<TopicEntity> actualEntities = entityCaptor.getValue();
//        assertEquals(1, actualEntities.size());
//        AtlasEntity actualEntity = actualEntities.get(0).toAtlasEntity();
//        assertEquals(expectedEntity.getTypeName(), actualEntity.getTypeName());
//        assertEquals(
//                expectedEntity.getAttribute(AtlasQualifiedName.ATTRIBUTE_QUALIFIED_NAME),
//                actualEntity.getAttribute(AtlasQualifiedName.ATTRIBUTE_QUALIFIED_NAME)
//        );
//        assertEquals(
//                expectedEntity.getAttribute(TopicEntity.REPLICATION_FACTOR),
//                actualEntity.getAttribute(TopicEntity.REPLICATION_FACTOR)
//        );
//    }
//
//    @Test
//    public void handleCreatePartition_connectorCalledWithExpectedTopics() {
//        Set<Auditor.AuditInformation> unused = Collections.emptySet();
//        Map<AclOperation, Set<Auditor.AuditInformation>> mockAudit = new HashMap<>();
//        mockAudit.put(AclOperation.ALTER, unused);
//
//        List<TopicEntity> allowedTopics = Collections.singletonList(
//                TopicEntity.withPartitionCount("topic", 3)
//        );
//        TopicEvent event = new TopicEvent(
//                Collections.singleton(TopicEvent.AuditedTopic.withPartitionNumber("topic", 3)),
//                TopicEvent.EventType.PARTITION_CHANGE);
//
//        when(entitySelector.selectTopicsForCreatePartition(any(), eq(unused))).thenReturn(allowedTopics);
//
//        kafkaHook.handleCreatePartition(event, mockAudit);
//
//        verify(connector).update(captor.capture());
//        List<TopicEntity> actualEntities = captor.getValue();
//        Assert.assertEquals(allowedTopics, actualEntities);
//    }
}
