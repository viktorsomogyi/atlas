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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TopicEntity extends AtlasKafkaEntity {

    public static final String KAFKA_TOPIC_TYPENAME = "kafka_topic";
    public static final String PARTITION_COUNT = "partitionCount";
    public static final String REPLICATION_FACTOR = "replicationFactor";

    private static final String DESCRIPTION_ATTR = "description";
    private static final String NAME = "name";
    private static final String URI = "uri";
    private static final String TOPIC = "topic";

    private static final int REPLICATION_FACTOR_NOT_PRESENT = -1;
    private static final int PARTITION_COUNT_NOT_PRESENT = -1;

    private final String topicName;
    private final int partitionCount;
    private final int replicationFactor;

    public TopicEntity(String topicName) {
        super(KAFKA_TOPIC_TYPENAME, topicName, LazyAtlasConfigStore.getInstance().getTopicMetadataNamespace());
        this.topicName = topicName;
        this.partitionCount = PARTITION_COUNT_NOT_PRESENT;
        this.replicationFactor = REPLICATION_FACTOR_NOT_PRESENT;
    }

    public TopicEntity(AtlasQualifiedName qualifiedName) {
        super(KAFKA_TOPIC_TYPENAME, qualifiedName);
        this.topicName = qualifiedName.getUniqueIdentifier();
        this.partitionCount = PARTITION_COUNT_NOT_PRESENT;
        this.replicationFactor = REPLICATION_FACTOR_NOT_PRESENT;
    }

    public TopicEntity(String topicName, int partitionCount, int replicationFactor) {
        super(KAFKA_TOPIC_TYPENAME, topicName, LazyAtlasConfigStore.getInstance().getTopicMetadataNamespace());
        this.topicName = topicName;
        this.partitionCount = partitionCount;
        this.replicationFactor = replicationFactor;
    }

    public static TopicEntity withReplicationFactor(String topicName, Integer replicationFactor) {
        return new TopicEntity(topicName, PARTITION_COUNT_NOT_PRESENT, replicationFactor);
    }

    public static TopicEntity withPartitionCount(String topicName, Integer partitionCount) {
        return new TopicEntity(topicName, partitionCount, REPLICATION_FACTOR_NOT_PRESENT);
    }

    @Override
    protected Map<String, Object> getAttributes() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TOPIC, topicName);
        attributes.put(NAME, topicName);
        attributes.put(DESCRIPTION_ATTR, topicName);
        attributes.put(URI, topicName);
        if (partitionCount != PARTITION_COUNT_NOT_PRESENT) {
            attributes.put(PARTITION_COUNT, partitionCount);
        }
        if (replicationFactor != REPLICATION_FACTOR_NOT_PRESENT) {
            attributes.put(REPLICATION_FACTOR, replicationFactor);
        }
        return attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        TopicEntity that = (TopicEntity) o;
        return partitionCount == that.partitionCount &&
                replicationFactor == that.replicationFactor &&
                topicName.equals(that.topicName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), topicName, partitionCount, replicationFactor);
    }
}
