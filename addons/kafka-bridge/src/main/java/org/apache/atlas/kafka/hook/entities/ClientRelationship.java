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

/**
 * A relationship between a producer or consumer and a topic or consumergroup. This represents the items stored in
 * a {@link org.apache.atlas.kafka.hook.ClientRelationshipCache}.
 * @param <T> is the type of the client (producer or consumer).
 * @param <U> is the type of the related object (topic or consumergroup).
 */
public class ClientRelationship<T extends AtlasKafkaClientEntity, U extends AtlasKafkaEntity> {

    private final T client;
    private final U relatedObject;

    public ClientRelationship(T client, U relatedObject) {
        this.client = client;
        this.relatedObject = relatedObject;
    }

    public T getClient() {
        return client;
    }

    public U getRelatedObject() {
        return relatedObject;
    }
}
