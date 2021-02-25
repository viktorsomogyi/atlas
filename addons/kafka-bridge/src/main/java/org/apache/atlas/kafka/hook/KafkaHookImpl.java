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

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.kafka.hook.entities.AtlasKafkaClientEntity;
import org.apache.atlas.kafka.hook.entities.AtlasKafkaEntity;
import org.apache.atlas.kafka.hook.entities.ClientLineageEntity;
import org.apache.atlas.kafka.hook.entities.ClientRelationship;
import org.apache.atlas.kafka.hook.entities.ConsumerEntity;
import org.apache.atlas.kafka.hook.entities.ConsumerGroupEntity;
import org.apache.atlas.kafka.hook.entities.ProducerEntity;
import org.apache.atlas.kafka.hook.entities.TopicEntity;
import org.apache.kafka.server.auditor.AuditEvent;
import org.apache.kafka.server.auditor.AuditInfo;
import org.apache.kafka.server.auditor.Auditor;
import org.apache.kafka.server.auditor.events.ProduceEvent;
import org.apache.kafka.server.auditor.events.SyncGroupEvent;
import org.apache.kafka.server.auditor.events.TopicEvent;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is the main implementation of the Atlas-Kafka hook. It is able to handle topic creation, topic deletion,
 * topic replication factor and partition count updates.
 */
public class KafkaHookImpl implements Auditor {

    private static final String AUDITORS_CONFIG_PREFIX = "auditors.";
    private static final String ATLAS_CONFIG_PREFIX = "atlas.";
    private static final String HOOK_CONNECTOR_CONFIG = "hook.connector";
    private static final String REST_CONNECTOR_CONFIG = "rest";
    private static final String KAFKA_CONNECTOR_CONFIG = "kafka";

    private final Logger LOG = LoggerFactory.getLogger(KafkaHookImpl.class.getName());
    private final Executor asyncTaskExecutor;

    private ClientRelationshipCache<ProducerEntity, TopicEntity> producerCache;
    private ClientRelationshipCache<ConsumerEntity, TopicEntity> consumerCache;
    private ClientRelationshipCache<ConsumerEntity, ConsumerGroupEntity> consumerGroupCache;
    private AtlasConnector atlasConnector;

    private final Predicate<Map.Entry<?, AuditInfo>> allowed = t -> t.getValue().errorCode() == 0
            && t.getValue().allowed() == AuthorizationResult.ALLOWED;

    /**
     * The plugin is instantiated with this constructor by org.apache.atlas.kafka.hook.KafkaHook through reflection.
     */
    public KafkaHookImpl() {
        asyncTaskExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setName("KafkaHookImpl_worker");
            thread.setUncaughtExceptionHandler((t, e) -> LOG.error("Uncaught exception on thread {}", t, e));
            return thread;
        });
    }

    @VisibleForTesting
    KafkaHookImpl(AtlasConnector atlasConnector,
                  ClientRelationshipCache<ProducerEntity, TopicEntity> producerCache,
                  ClientRelationshipCache<ConsumerEntity, TopicEntity> consumerCache,
                  ClientRelationshipCache<ConsumerEntity, ConsumerGroupEntity> consumerGroupCache) {
        this();
        this.atlasConnector = atlasConnector;
        this.producerCache = producerCache;
        this.consumerCache = consumerCache;
        this.consumerGroupCache = consumerGroupCache;
    }

    @Override
    public void close() {
        if (atlasConnector != null) {
            atlasConnector.close();
        }
    }

    @Override
    public void audit(AuditEvent event, AuthorizableRequestContext requestContext) {
        asyncTaskExecutor.execute(() -> auditInternal(event, requestContext));
    }

    @VisibleForTesting
    void auditInternal(AuditEvent event, AuthorizableRequestContext requestContext) {
        if (event instanceof TopicEvent) {
            switch (((TopicEvent) event).eventType()) {
                case CREATE:
                    createTopics((TopicEvent) event);
                    break;
                case DELETE:
                    deleteTopics((TopicEvent) event);
                    break;
//                case PARTITION_CHANGE:
//                    handleCreatePartition((TopicEvent) event);
//                    break;
//                case REPLICATION_FACTOR_CHANGE:
//                    handleReplicationFactorChange((TopicEvent) event);
//                    break;
            }
        } else if (event instanceof ProduceEvent) {
            handleProduce((ProduceEvent) event, requestContext);
        } else if (event instanceof SyncGroupEvent) {
            handleConsume((SyncGroupEvent) event, requestContext);
        }
    }

    @VisibleForTesting
    void createTopics(TopicEvent event) {
        List<TopicEntity> topicsToBeCreated = event.auditInfo().entrySet().stream()
                .filter(allowed)
                .map(entry ->
                        new TopicEntity(entry.getKey().name(), entry.getKey().numPartitions(), entry.getKey().replicationFactor()))
                .collect(Collectors.toList());
        if (topicsToBeCreated.isEmpty()) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating topics {}", topicsToBeCreated.stream().map(AtlasKafkaEntity::toString).collect(Collectors.joining(", ")));
        }
        atlasConnector.create(topicsToBeCreated);
    }

    @VisibleForTesting
    void deleteTopics(TopicEvent event) {
        List<TopicEntity> topicsToBeDeleted = event.auditInfo().entrySet().stream()
                .filter(allowed)
                .map(entry -> new TopicEntity(entry.getKey().name()))
                .collect(Collectors.toList());
        if (topicsToBeDeleted.isEmpty()) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting topics {}", topicsToBeDeleted.stream().map(AtlasKafkaEntity::toString).collect(Collectors.joining(", ")));
        }
        atlasConnector.delete(topicsToBeDeleted);
    }

    @VisibleForTesting
    void handleProduce(ProduceEvent event, AuthorizableRequestContext requestContext) {
        List<TopicEntity> allowedTopics = event.topicAuditInfo().entrySet().stream()
                .filter(allowed)
                .map(auditInfo -> new TopicEntity(auditInfo.getKey().topic()))
                .collect(Collectors.toList());
        ProducerEntity producerEntity = new ProducerEntity(requestContext.clientId());
        createOrUpdateClientAndRelationship(producerEntity, allowedTopics, producerCache);
    }

    @VisibleForTesting
    void handleConsume(SyncGroupEvent event, AuthorizableRequestContext requestContext) {
        ConsumerGroupEntity consumerGroupEntity = new ConsumerGroupEntity(event.groupId());
        ConsumerEntity consumerEntity = new ConsumerEntity(requestContext.clientId());
        Optional<ClientRelationship<ConsumerEntity, ConsumerGroupEntity>> relationship = consumerGroupCache
                .putRelationship(consumerEntity, consumerGroupEntity);
        if (relationship.isPresent()) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Creating consumer group {}", consumerGroupEntity);
                }
                atlasConnector.create(consumerGroupEntity);
            } catch (Exception e) {
                LOG.error("Couldn't add consumer group " + consumerGroupEntity.getQualifiedName(), e);
                consumerGroupCache.invalidateAll(Collections.singletonList(relationship.get()));
            }
        }
        consumerEntity.setGroupRelationship(consumerGroupEntity.toAtlasEntity());
        List<TopicEntity> assignedTopics = event.assignedTopics()
                .stream()
                .map(TopicEntity::new)
                .collect(Collectors.toList());
        createOrUpdateClientAndRelationship(consumerEntity, assignedTopics, consumerCache);
    }

    @VisibleForTesting
    void handleCreatePartition(TopicEvent event) {
        List<TopicEntity> allowedTopics = event.auditInfo().entrySet().stream()
                .filter(allowed)
                .map(topic -> TopicEntity.withPartitionCount(topic.getKey().name(), topic.getKey().numPartitions()))
                .collect(Collectors.toList());
        if (allowedTopics.isEmpty()) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Handling partition update {}", allowedTopics.stream().map(AtlasKafkaEntity::toString).collect(Collectors.joining(", ")));
        }
        atlasConnector.update(allowedTopics);
    }

    @VisibleForTesting
    void handleReplicationFactorChange(TopicEvent event) {
        List<TopicEntity> updatedTopics = event.auditInfo().entrySet().stream()
                .filter(allowed)
                .map(topic -> TopicEntity.withReplicationFactor(topic.getKey().name(), topic.getKey().replicationFactor()))
                .collect(Collectors.toList());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Handling replication factor update {}", updatedTopics.stream()
                    .map(AtlasKafkaEntity::toString).collect(Collectors.joining(", ")));
        }
        if (updatedTopics.isEmpty()) {
            return;
        }
        atlasConnector.update(updatedTopics);
    }

    <T extends AtlasKafkaClientEntity> void createOrUpdateClientAndRelationship(
            T clientEntity,
            List<TopicEntity> assignedTopics,
            ClientRelationshipCache<T, TopicEntity> cache) {
        if (assignedTopics.isEmpty()) {
            return;
        }
        boolean clientExists = cache.clientExists(clientEntity);
        List<ClientRelationship<T, TopicEntity>> newRelationships =
                createMissingRelationships(clientEntity, assignedTopics, cache);
        if (!newRelationships.isEmpty()) {
            //Create or update client entity
            try {
                ClientLineageEntity clientLineageEntity = ClientLineageEntity.of(clientEntity, assignedTopics);
                if (clientExists) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Updating client {}", clientEntity);
                    }
                    atlasConnector.update(clientEntity);
                    atlasConnector.update(clientLineageEntity);
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Adding client {}", clientEntity);
                    }
                    atlasConnector.create(clientEntity);
                    atlasConnector.create(clientLineageEntity);
                }
            } catch (Exception e) {
                cache.invalidateAll(newRelationships);
            }
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // On startup we asynchronously synchronize Atlas with the topics in Kafka
        Map<String, Object> auditorConfig = getConfigsOf(AUDITORS_CONFIG_PREFIX, configs);
        consumerCache = new ClientRelationshipCache<>(20000);
        producerCache = new ClientRelationshipCache<>(20000);
        consumerGroupCache = new ClientRelationshipCache<>(20000);
        //If you want to test with REST client you'll need this
        Map<String, Object> atlasAuditorRelatedKafkaConfigs = getConfigsOf(ATLAS_CONFIG_PREFIX, auditorConfig);
        String hookConnectorConfig = (String) atlasAuditorRelatedKafkaConfigs.get(HOOK_CONNECTOR_CONFIG);
        if (REST_CONNECTOR_CONFIG.equals(hookConnectorConfig)) {
            this.atlasConnector = new RestApiConnector(atlasAuditorRelatedKafkaConfigs);
        } else if (hookConnectorConfig == null || KAFKA_CONNECTOR_CONFIG.equals(hookConnectorConfig)) {
            this.atlasConnector = new KafkaHookNotification();
        } else {
            throw new RuntimeException(String.format("Invalid configuration for %s: %s", HOOK_CONNECTOR_CONFIG, hookConnectorConfig));
        }
    }

    private <T extends AtlasKafkaClientEntity> List<ClientRelationship<T, TopicEntity>> createMissingRelationships(
            T client,
            List<TopicEntity> qualifiedTopicNames,
            ClientRelationshipCache<T, TopicEntity> cache) {
        List<ClientRelationship<T, TopicEntity>> newlyAddedRelationships = qualifiedTopicNames.stream()
                .flatMap(topic -> {
                    Optional<ClientRelationship<T, TopicEntity>> relationship = cache
                            .putRelationship(client, topic);
                    return relationship.map(Stream::of).orElseGet(Stream::empty);
                })
                .collect(Collectors.toList());

        if (newlyAddedRelationships.isEmpty()) {
            return Collections.emptyList();
        }

        return newlyAddedRelationships;
    }

    static Map<String, Object> getConfigsOf(String prefix, Map<String, ?> configMap) {
        Map<String, Object> props = new HashMap<>();
        for (Map.Entry<String, ?> entry : configMap.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                props.put(entry.getKey().replace(prefix, ""), entry.getValue());
            }
        }
        return props;
    }
}
