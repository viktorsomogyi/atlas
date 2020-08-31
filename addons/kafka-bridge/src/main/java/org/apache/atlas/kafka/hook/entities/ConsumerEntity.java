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

package org.apache.atlas.kafka.hook.entities;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.type.AtlasTypeUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates a Kafka consumer entity that is identified by its client ID.
 */
public class ConsumerEntity extends AtlasKafkaClientEntity {

    private static final String KAFKA_CONSUMER_TYPENAME = "kafka_consumer";
    private static final String KAFKA_CONSUMER_LINEAGE_TYPENAME = "kafka_consumer_lineage";
    private static final String NAME = "name";
    private static final String CLIENT_ID = "clientId";

    private static final String ATTRIBUTE_CONSUMER_GROUPS = "kafkaConsumerGroups";
    private static final String RELATIONSHIP_CONSUMER_CONSUMER_GROUP_TOPIC = "kafka_consumer_consumer_group_relationship";

    public ConsumerEntity(String clientId) {
        super(KAFKA_CONSUMER_TYPENAME, clientId);
    }

    public void setGroupRelationship(AtlasEntity consumerGroup) {
        toAtlasEntity().setRelationshipAttribute(ATTRIBUTE_CONSUMER_GROUPS,
                AtlasTypeUtil.getAtlasRelatedObjectIds(Collections.singletonList(consumerGroup), RELATIONSHIP_CONSUMER_CONSUMER_GROUP_TOPIC));
    }

    @Override
    protected Map<String, Object> getAttributes() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(NAME, getQualifiedName().getUniqueIdentifier());
        attributes.put(CLIENT_ID, getQualifiedName().getUniqueIdentifier());
        return attributes;
    }

    @Override
    public ClientLineageEntity createClientLineageEntity(String lineageName, List<AtlasEntity> topicEntities) {
        ClientLineageEntity producerLineageEntity = new ClientLineageEntity(KAFKA_CONSUMER_LINEAGE_TYPENAME, lineageName);
        producerLineageEntity.toAtlasEntity().setRelationshipAttribute(ATTRIBUTE_OUTPUTS,
                AtlasTypeUtil.getAtlasRelatedObjectIds(Collections.singletonList(toAtlasEntity()), RELATIONSHIP_PROCESS_DATASET_OUTPUTS));
        producerLineageEntity.toAtlasEntity().setRelationshipAttribute(ATTRIBUTE_INPUTS,
                AtlasTypeUtil.getAtlasRelatedObjectIds(topicEntities, RELATIONSHIP_DATASET_PROCESS_INPUTS));
        return producerLineageEntity;    }
}
