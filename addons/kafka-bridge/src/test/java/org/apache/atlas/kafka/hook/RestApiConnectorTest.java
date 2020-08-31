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

import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.kafka.hook.entities.LazyAtlasConfigStore;
import org.apache.atlas.kafka.hook.entities.TopicEntity;
import org.apache.atlas.model.discovery.AtlasQuickSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class RestApiConnectorTest {

    private final AtlasClientV2 atlasClientV2 = mock(AtlasClientV2.class);

    private RestApiConnector restApiConnector;
    private String defaultMetadataNamespace;

    @Captor
    ArgumentCaptor<AtlasEntity.AtlasEntitiesWithExtInfo> atlasEntitiesCaptor;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        restApiConnector = new RestApiConnector(atlasClientV2);
        defaultMetadataNamespace = LazyAtlasConfigStore.getInstance().getTopicMetadataNamespace();
    }

    @Test
    public void createTopics_TopicEmpty_AtlasClientNotCalled() {
        restApiConnector.create(new ArrayList<>());

        verifyNoInteractions(atlasClientV2);
    }

    @Test
    public void createTopics_TopicCreationContainsEntity_AtlasClientCalledWithThem() throws AtlasServiceException {
        AtlasEntity firstTopic = new AtlasEntity("kafka_topic");
        firstTopic.setAttribute("topic", "first");
        AtlasEntity secondTopic = new AtlasEntity("kafka_topic");
        secondTopic.setAttribute("topic", "second");
        List<TopicEntity> topicEntities = Arrays.asList(new TopicEntity("first"), new TopicEntity("second"));

        restApiConnector.create(topicEntities);

        verify(atlasClientV2, times(1))
                .createEntities(atlasEntitiesCaptor.capture());

        AtlasEntity.AtlasEntitiesWithExtInfo actualCreateArgs = atlasEntitiesCaptor.getValue();
        assertThat(actualCreateArgs.getEntities().size(), is(2));
        AtlasEntity entity0 = actualCreateArgs.getEntities().get(0);
        assertThat(entity0.getAttribute("qualifiedName"), is("first@" + defaultMetadataNamespace));
        AtlasEntity entity1 = actualCreateArgs.getEntities().get(1);
        assertThat(entity1.getAttribute("qualifiedName"), is("second@" + defaultMetadataNamespace));
    }

    @Test
    public void deleteTopics_TopicDeleteNull_AtlasClientNotCalled() {
        restApiConnector.delete(new ArrayList<>());

        verifyNoInteractions(atlasClientV2);
    }

    @Test
    public void deleteTopics_TopicDeleteContainsErroredEntity_AtlasClientNotCalled() {
        List<TopicEntity> entities = new ArrayList<>();
        restApiConnector.delete(entities);

        verifyNoInteractions(atlasClientV2);
    }

    @Test
    public void deleteTopics_TopicDeleteContainsEntity_AtlasClientCalledWithThem() throws AtlasServiceException {
        List<TopicEntity> entitiesToBeDeleted = new ArrayList<>();
        entitiesToBeDeleted.add(new TopicEntity("first"));
        entitiesToBeDeleted.add(new TopicEntity("second"));

        Map<String, Object> expectedEntities = new HashMap<>();
        entitiesToBeDeleted.forEach(e -> e.getQualifiedName().addNameToAttributeMap(expectedEntities));

        restApiConnector.delete(entitiesToBeDeleted);

        verify(atlasClientV2, times(1))
                .deleteEntityByAttribute(eq("kafka_topic"),
                        eq(expectedEntities.entrySet()
                                .stream().collect(Collectors.toMap(Map.Entry::getKey, o -> (String) o.getValue()))));
    }

    @Test()
    public void deleteTopics_AtlasClientThrowsException_ExceptionSwallowed() throws AtlasServiceException {
        List<TopicEntity> expectedEntities = new ArrayList<>();
        expectedEntities.add(new TopicEntity("first"));
        expectedEntities.add(new TopicEntity("second"));

//        when(entitySelector.selectEntitiesToBeDeleted(any())).thenReturn(expectedEntities);
        when(atlasClientV2.createEntities(any())).thenThrow(AtlasServiceException.class);

        restApiConnector.delete(expectedEntities);
    }

    @Test
    public void synchronizeTopics_emptyKafkaTopicsDescriptions_noCallToAtlas() {
        Map<String, TopicDescription> topicsInKafka = new HashMap<>();

        restApiConnector.synchronizeTopics(topicsInKafka);

        verifyNoInteractions(atlasClientV2);
    }

    @Test
    public void synchronizeTopics_sameTopicsPresent_noCallToAtlas() throws AtlasServiceException {
        Map<String, TopicDescription> topicsInKafka = new HashMap<>();
        topicsInKafka.put("topic-1", topicDescription("topic-1"));
        topicsInKafka.put("topic-2", topicDescription("topic-2"));

        List<AtlasEntityHeader> entities = atlasTopics(Arrays.asList("topic-1", "topic-2"));
        AtlasQuickSearchResult atlasResult = atlasQuickSearchTopics(entities);
        restApiConnector = new RestApiConnector(atlasClientV2);

        when(atlasClientV2.quickSearch(anyString(), anyString(), anyBoolean(), anyInt(), anyInt()))
                .thenReturn(atlasResult);

        restApiConnector.synchronizeTopics(topicsInKafka);

        verify(atlasClientV2, times(0)).createEntities(any());
        verify(atlasClientV2, times(0)).deleteEntityByAttribute(any(), any());
    }

    @Test
    public void synchronizeTopics_atlasContainsAllTopicsFromKafkaAndTopicsInDifferentMetadataNamespace_noCallToAtlas() throws AtlasServiceException {
        String differentNamespace = "different";
        Map<String, TopicDescription> topicsInKafka = new HashMap<>();
        topicsInKafka.put("topic-1", topicDescription("topic-1"));
        topicsInKafka.put("topic-2", topicDescription("topic-2"));


        List<AtlasEntityHeader> sameNamespaceEntities =
                atlasTopics(Arrays.asList("topic-1", "topic-2"));
        List<AtlasEntityHeader> differentNamespaceEntities =
                atlasTopics(Arrays.asList("topic-1", "topic-2"), differentNamespace);
        List<AtlasEntityHeader> allEntities = new ArrayList<>(sameNamespaceEntities);
        allEntities.addAll(differentNamespaceEntities);
        sameNamespaceEntities.addAll(allEntities);
        AtlasQuickSearchResult atlasResult = atlasQuickSearchTopics(sameNamespaceEntities);

        when(atlasClientV2.quickSearch(anyString(), anyString(), anyBoolean(), anyInt(), anyInt()))
                .thenReturn(atlasResult);
        restApiConnector = restApiConnector();

        restApiConnector.synchronizeTopics(topicsInKafka);

        verify(atlasClientV2, times(0)).createEntities(any());
        verify(atlasClientV2, times(0)).deleteEntityByAttribute(any(), any());
    }

    @Test
    public void synchronizeTopics_atlasContainsTopicsNotInKafkaInSameNamespace_extraTopicsDeleted() throws AtlasServiceException {
        Map<String, TopicDescription> topicsInKafka = new HashMap<>();
        topicsInKafka.put("topic-1", topicDescription("topic-1"));
        topicsInKafka.put("topic-2", topicDescription("topic-2"));

        List<AtlasEntityHeader> sameNamespaceEntities =
                atlasTopics(Arrays.asList("topic-1", "topic-2", "topic-extra"));
        AtlasQuickSearchResult atlasResult = atlasQuickSearchTopics(sameNamespaceEntities);
        when(atlasClientV2.quickSearch(anyString(), anyString(), anyBoolean(), anyInt(), anyInt()))
                .thenReturn(atlasResult);
        restApiConnector = restApiConnector();

        Map<String, String> expectedDeletion = new HashMap<>();
        expectedDeletion.put("qualifiedName", "topic-extra" + "@" + defaultMetadataNamespace);


        restApiConnector.synchronizeTopics(topicsInKafka);

        verify(atlasClientV2, times(1))
                .deleteEntityByAttribute(eq("kafka_topic"), eq(expectedDeletion));
    }

    @Test
    public void synchronizeTopics_KafkaContainsTopicsNotInAtlas_topicsCreated() throws AtlasServiceException {
        Map<String, TopicDescription> topicsInKafka = new HashMap<>();
        topicsInKafka.put("topic-1", topicDescription("topic-1"));
        topicsInKafka.put("topic-2", topicDescription("topic-2"));

        List<AtlasEntityHeader> sameNamespaceEntities =
                atlasTopics(Collections.singletonList("topic-1"));
        AtlasQuickSearchResult atlasResult = atlasQuickSearchTopics(sameNamespaceEntities);
        when(atlasClientV2.quickSearch(anyString(), anyString(), anyBoolean(), anyInt(), anyInt()))
                .thenReturn(atlasResult);
        restApiConnector = restApiConnector();

        restApiConnector.synchronizeTopics(topicsInKafka);

        verify(atlasClientV2, times(1))
                .createEntities(atlasEntitiesCaptor.capture());

        AtlasEntity.AtlasEntitiesWithExtInfo actualCreateArgs = atlasEntitiesCaptor.getValue();
        assertThat(actualCreateArgs.getEntities().size(), is(1));
        AtlasEntity actualEntity = actualCreateArgs.getEntities().get(0);
        assertThat(actualEntity.getAttribute("qualifiedName"), is("topic-2@" + defaultMetadataNamespace));
    }

    @Test
    public void synchronizeTopics_KafkaContainsATopicNotInAtlas_topicCreatedWithInfoFromDescription() throws AtlasServiceException {
        Map<String, TopicDescription> topicsInKafka = new HashMap<>();
        topicsInKafka.put("topic", topicDescription("topic", 2, 3));

        AtlasQuickSearchResult atlasResult = atlasQuickSearchTopics(Collections.emptyList());
        when(atlasClientV2.quickSearch(anyString(), anyString(), anyBoolean(), anyInt(), anyInt()))
                .thenReturn(atlasResult);
        restApiConnector = restApiConnector();

        restApiConnector.synchronizeTopics(topicsInKafka);

        verify(atlasClientV2, times(1))
                .createEntities(atlasEntitiesCaptor.capture());

        AtlasEntity.AtlasEntitiesWithExtInfo actualCreateArgs = atlasEntitiesCaptor.getValue();
        assertThat(actualCreateArgs.getEntities().size(), is(1));
        AtlasEntity actualEntity = actualCreateArgs.getEntities().get(0);
        assertThat(actualEntity.getAttribute("qualifiedName"), is("topic@" + defaultMetadataNamespace));
        assertThat(actualEntity.getAttribute("partitionCount"), is(2));
        assertThat(actualEntity.getAttribute("replicationFactor"), is(3));
        assertThat(actualEntity.getAttribute("clusterName"), is(defaultMetadataNamespace));

    }

    @Test
    public void synchronizeTopics_KafkaTopicNamesContainedInAtlasWithDifferentNamespace_topicsCreated() throws AtlasServiceException {
        String differentNamespace = "different";
        Map<String, TopicDescription> topicsInKafka = new HashMap<>();
        topicsInKafka.put("topic-1", topicDescription("topic-1"));
        topicsInKafka.put("topic-2", topicDescription("topic-2"));

        List<AtlasEntityHeader> differentNamespaceEntities =
                atlasTopics(Arrays.asList("topic-1", "topic-2"), differentNamespace);
        AtlasQuickSearchResult atlasResult = atlasQuickSearchTopics(differentNamespaceEntities);
        when(atlasClientV2.quickSearch(anyString(), anyString(), anyBoolean(), anyInt(), anyInt()))
                .thenReturn(atlasResult);
        restApiConnector = restApiConnector();

        restApiConnector.synchronizeTopics(topicsInKafka);

        verify(atlasClientV2, times(1))
                .createEntities(atlasEntitiesCaptor.capture());

        AtlasEntity.AtlasEntitiesWithExtInfo actualCreateArgs = atlasEntitiesCaptor.getValue();
        assertThat(actualCreateArgs.getEntities().size(), is(2));
        AtlasEntity actualEntity1 = actualCreateArgs.getEntities().get(0);
        assertThat(actualEntity1.getAttribute("qualifiedName"), is("topic-1@" + defaultMetadataNamespace));
        AtlasEntity actualEntity2 = actualCreateArgs.getEntities().get(1);
        assertThat(actualEntity2.getAttribute("qualifiedName"), is("topic-2@" + defaultMetadataNamespace));
    }

    /**
     * Creates a TopicDescription with a replication factor of 1
     */
    private TopicDescription topicDescription(String name) {
        return topicDescription(name, 1, 1);
    }

    /**
     * Creates a TopicDescription with a given replication and partition count
     */
    private TopicDescription topicDescription(String name, int partitionCount, int replicationFactor) {
        List<Node> replicas = new ArrayList<>();
        for (int i=0; i< replicationFactor; ++i) {
            replicas.add(new Node(i, "unused", i));
        }

        List<TopicPartitionInfo> partitions = new ArrayList<>();
        for (int i=0; i< partitionCount; ++i) {
            partitions.add(new TopicPartitionInfo(i, null, replicas, replicas));
        }

        return new TopicDescription(name, true, partitions);
    }

    private AtlasQuickSearchResult atlasQuickSearchTopics(List<AtlasEntityHeader> entities) {
        AtlasSearchResult searchResult = new AtlasSearchResult();
        searchResult.setEntities(entities);

        return new AtlasQuickSearchResult(searchResult, Collections.emptyMap());
    }


    private List<AtlasEntityHeader> atlasTopics(List<String> names) {
        return names.stream()
                .map(this::atlasTopic)
                .collect(Collectors.toList());
    }

    private List<AtlasEntityHeader> atlasTopics(List<String> names, String namespace) {
        return names.stream()
                .map(name -> atlasTopic(name, namespace))
                .collect(Collectors.toList());
    }

    private AtlasEntityHeader atlasTopic(String name) {
        Map<String, Object> attributes = new HashMap<>();
        String qualifiedName = name + "@" + defaultMetadataNamespace;
        attributes.put("qualifiedName", qualifiedName);

        return new AtlasEntityHeader("kafka_topic", attributes);
    }

    private AtlasEntityHeader atlasTopic(String name, String namespace) {
        Map<String, Object> attributes = new HashMap<>();
        String qualifiedName = name + "@" + namespace;
        attributes.put("qualifiedName", qualifiedName);

        return new AtlasEntityHeader("kafka_topic", attributes);
    }

    private RestApiConnector restApiConnector() {
        return new RestApiConnector(atlasClientV2);

    }
}
