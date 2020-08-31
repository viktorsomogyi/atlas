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
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.kafka.hook.entities.AtlasKafkaClientEntity;
import org.apache.atlas.kafka.hook.entities.AtlasKafkaEntity;
import org.apache.atlas.kafka.hook.entities.AtlasQualifiedName;
import org.apache.atlas.kafka.hook.entities.ClientLineageEntity;
import org.apache.atlas.kafka.hook.entities.ConsumerGroupEntity;
import org.apache.atlas.kafka.hook.entities.LazyAtlasConfigStore;
import org.apache.atlas.kafka.hook.entities.TopicEntity;
import org.apache.atlas.model.discovery.AtlasQuickSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class RestApiConnector implements AtlasConnector {

    private static final String ATLAS_ENDPOINT = "atlas.rest.address";
    private static final String DEFAULT_ATLAS_URL = "http://localhost:21000/";
    private static final String ATLAS_USERNAME_CONFIG = "user.name";
    private static final String ATLAS_PASSWORD_CONFIG = "password";

    public static final int DISCOVERY_LIMIT = 20000;
    public static final int ZERO_OFFSET = 0;

    private final Logger LOG = LoggerFactory.getLogger(RestApiConnector.class.getName());
    private final AtlasClientV2 atlasClientV2;

    public RestApiConnector(Map<String, Object> properties) {
        try {
            this.atlasClientV2 = createAtlasClient(properties);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    RestApiConnector(AtlasClientV2 atlasClientV2) {
        this.atlasClientV2 = atlasClientV2;
    }

    @Override
    public void create(List<TopicEntity> topics) {
        createEntities(topics.stream().map(TopicEntity::toAtlasEntity).collect(Collectors.toList()));
    }

    @Override
    public void delete(List<TopicEntity> topics) {
        deleteEntitiesFromAtlas(topics);
    }

    @Override
    public void create(AtlasKafkaClientEntity client) {
        createEntity(client.toAtlasEntity());
    }

    @Override
    public void update(AtlasKafkaClientEntity client) {
        updateEntities(Collections.singletonList(client.toAtlasEntity()));
    }

    @Override
    public void create(ClientLineageEntity clientLineage) {
        createEntity(clientLineage.toAtlasEntity());
    }

    @Override
    public void update(ClientLineageEntity clientLineage) {
        updateEntities(Collections.singletonList(clientLineage.toAtlasEntity()));
    }

    @Override
    public void update(List<TopicEntity> topics) {
        updateEntities(topics.stream().map(TopicEntity::toAtlasEntity).collect(Collectors.toList()));
    }

    @Override
    public void create(ConsumerGroupEntity consumerGroup) {
        createEntity(consumerGroup.toAtlasEntity());
    }

    @Override
    public void close() {
        if (atlasClientV2 != null) {
            atlasClientV2.close();
        }
    }

    private <T extends AtlasKafkaEntity> void deleteEntitiesFromAtlas(List<T> entities) {
        entities.forEach(entity -> {
            try {
                atlasClientV2.deleteEntityByAttribute(entity.toAtlasEntity().getTypeName(),
                        entity.getQualifiedName().getAsAttributeMap());
                LOG.info("Deleting entity from Atlas: {}", entity);
            } catch (AtlasServiceException e) {
                LOG.warn("Couldn't delete entity from Atlas: {}", entity);
            }
        });
    }

    private void createEntity(AtlasEntity entity) {
        createEntities(Collections.singletonList(entity));
    }

    private void createEntities(List<AtlasEntity> entities) {
        if (entities.isEmpty()) {
            return;
        }

        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo(entities);
        try {
            EntityMutationResponse response = atlasClientV2.createEntities(entitiesWithExtInfo);
            if (response != null && response.getFirstEntityCreated() != null) {
                LOG.info("Added {}", entities);
            } else {
                LOG.warn("Couldn't add {}", entities);
            }
        } catch (AtlasServiceException e) {
            LOG.warn("Couldn't create " + entities + " in Atlas", e);
        }
    }

    private void updateEntities(List<AtlasEntity> entities) {
        if (entities.isEmpty()) {
            return;
        }

        try {
            EntityMutationResponse response = atlasClientV2.updateEntities(new AtlasEntity.AtlasEntitiesWithExtInfo(entities));
            if (response != null && response.getFirstEntityUpdated() != null) {
                LOG.info("Added {}", entities);
            } else {
                LOG.warn("Couldn't update {}", entities);
            }
        } catch (AtlasServiceException e) {
            LOG.warn("Couldn't create " + entities + " in Atlas", e);
        }
    }

    /**
     * Creates an deletes topics in Atlas based on the current topic descriptions found in Kafka
     * If the empty map is provided, nothing is deleted from Atlas
     *
     * @param topicsInKafka all the topics existing in Kafka
     */
    public void synchronizeTopics(Map<String, TopicDescription> topicsInKafka) {
        if (topicsInKafka.isEmpty()) {
            return;
        }

        try {
            List<AtlasEntityHeader> topicsInAtlas = retrieveExistingTopics();
            List<AtlasEntity> topicsToCreate = topicsJustInKafka(topicsInAtlas, topicsInKafka);

            if (!topicsToCreate.isEmpty()) {
                atlasClientV2.createEntities(new AtlasEntity.AtlasEntitiesWithExtInfo(topicsToCreate));
            }

            List<AtlasQualifiedName> topicsNotInKafka = topicsJustInAtlas(topicsInAtlas, topicsInKafka);
            List<AtlasQualifiedName> topicsToDelete = topicsJustInMetadataNamespace(topicsNotInKafka);
            topicsToDelete.forEach(topicQualifiedName -> {
                try {
                    atlasClientV2.deleteEntityByAttribute(TopicEntity.KAFKA_TOPIC_TYPENAME, topicQualifiedName.getAsAttributeMap());
                } catch (AtlasServiceException e) {
                    LOG.warn("Couldn't delete Atlas topic: {}", topicQualifiedName, e);
                }
            });
        } catch (AtlasServiceException e) {
            LOG.warn("Couldn't synchronize Atlas with Kafka topics on startup", e);
        }
    }

    private List<AtlasEntityHeader> retrieveExistingTopics() throws AtlasServiceException {
        AtlasQuickSearchResult result = atlasClientV2.quickSearch("", TopicEntity.KAFKA_TOPIC_TYPENAME, true, DISCOVERY_LIMIT, ZERO_OFFSET);

        return new ArrayList<>(result.getSearchResults().getEntities());
    }

    private List<AtlasEntity> topicsJustInKafka(List<AtlasEntityHeader> atlasTopicHeaders, Map<String, TopicDescription> topicDescriptionMap) {
        Set<TopicEntity> atlasTopicQualifiedNames = atlasTopicHeaders.stream()
                .map(AtlasQualifiedName::of)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(TopicEntity::new)
                .collect(Collectors.toSet());

        return topicDescriptionMap.keySet()
                .stream()
                .map(TopicEntity::new)
                .filter(topic -> !atlasTopicQualifiedNames.contains(topic))
                .map(topic -> {
                    TopicDescription td = topicDescriptionMap.get(topic.getQualifiedName().getUniqueIdentifier());
                    int partitions = td.partitions().size();
                    int replicationFactor = td.partitions().get(0).replicas().size();
                    return new TopicEntity(td.name(), partitions, replicationFactor).toAtlasEntity();
                }).collect(Collectors.toList());
    }

    private List<AtlasQualifiedName> topicsJustInAtlas(List<AtlasEntityHeader> atlasTopicHeaders, Map<String, TopicDescription> topicDescriptionMap) {
        Set<AtlasQualifiedName> qualifiedTopicsExistingInKafka = topicDescriptionMap.keySet()
                .stream()
                .map(t -> new TopicEntity(t).getQualifiedName())
                .collect(Collectors.toSet());

        return atlasTopicHeaders
                .stream()
                .map(AtlasQualifiedName::of)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(atlasQualifiedName -> !qualifiedTopicsExistingInKafka.contains(atlasQualifiedName))
                .collect(Collectors.toList());
    }

    private List<AtlasQualifiedName> topicsJustInMetadataNamespace(List<AtlasQualifiedName> atlasQualifiedNames) {
        String metadataNamespace = LazyAtlasConfigStore.getInstance().getTopicMetadataNamespace();
        return atlasQualifiedNames.stream()
                .filter(atlasQualifiedName -> metadataNamespace.equals(atlasQualifiedName.getMetadataNamespace()))
                .collect(Collectors.toList());
    }

    private AtlasClientV2 createAtlasClient(Map<String, Object> propertiesFromKafka) throws IOException {
        String[] urls = new String[0];
        try {
            urls = ApplicationProperties.get().getStringArray(ATLAS_ENDPOINT);
        } catch (AtlasException e) {
            LOG.warn("Couldn't find Atlas config, using default values", e);
        }
        if (urls == null || urls.length == 0) {
            urls = new String[]{DEFAULT_ATLAS_URL};
        }
        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            LOG.warn("THIS MODE IS NOT RECOMMENDED FOR PRODUCTION USAGE");
            String username = (String) propertiesFromKafka.get(ATLAS_USERNAME_CONFIG);
            String password = (String) propertiesFromKafka.get(ATLAS_PASSWORD_CONFIG);
            return new AtlasClientV2(urls, new String[]{username, password});
        } else {
            UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
            return new AtlasClientV2(ugi, ugi.getShortUserName(), urls);
        }
    }
}
