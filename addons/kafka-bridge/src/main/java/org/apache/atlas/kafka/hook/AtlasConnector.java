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

import org.apache.atlas.kafka.hook.entities.AtlasKafkaClientEntity;
import org.apache.atlas.kafka.hook.entities.AtlasKafkaEntity;
import org.apache.atlas.kafka.hook.entities.ClientLineageEntity;
import org.apache.atlas.kafka.hook.entities.ConsumerGroupEntity;
import org.apache.atlas.kafka.hook.entities.TopicEntity;
import org.apache.atlas.model.instance.AtlasEntity;

import java.util.List;

/**
 * An interface to define what functionality is supported by the Kafka-Atlas hook.
 */
public interface AtlasConnector {
    void create(List<TopicEntity> topics);
    void delete(List<TopicEntity> topics);
    void update(List<TopicEntity> topics);
    void create(AtlasKafkaClientEntity client);
    void update(AtlasKafkaClientEntity client);
    void create(ConsumerGroupEntity consumerGroup);
    void create(ClientLineageEntity clientLineage);
    void update(ClientLineageEntity clientLineage);

    void close();
}
