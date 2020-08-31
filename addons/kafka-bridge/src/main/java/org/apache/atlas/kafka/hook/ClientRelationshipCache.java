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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.atlas.kafka.hook.entities.AtlasKafkaClientEntity;
import org.apache.atlas.kafka.hook.entities.AtlasKafkaEntity;
import org.apache.atlas.kafka.hook.entities.ClientRelationship;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A cache that holds the clientId -> List(objectName) relationships in a cache. The cache is capped by a maximum size
 * where elements are expired based on the Window TinyLfu policy (https://github.com/ben-manes/caffeine/wiki/Efficiency).
 *
 * The cache can be used for storing client -> topic relationships or consumer -> consumergroup relationships.
 */
public class ClientRelationshipCache<T extends AtlasKafkaClientEntity, U extends AtlasKafkaEntity> {

    private final Cache<T, Set<U>> cache;

    public ClientRelationshipCache(int maxSize) {
        cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .initialCapacity(maxSize / 2)
                .removalListener((t, us, removalCause) -> { })
                .build();
    }

    /**
     * Checks if a client already exists in the cache.
     * @param client the client to search. The equals method will be used for matching.
     * @return a boolean value which is true if the client exists, false otherwise.
     */
    public boolean clientExists(T client) {
        return cache.getIfPresent(client) != null;
    }

    /**
     * Invalidates the items in the cache, practically deletes them.
     * @param relationships a list of relationships to delete.
     */
    public void invalidateAll(Iterable<ClientRelationship<T, U>> relationships) {
        cache.invalidateAll(relationships);
    }

    /**
     * Creates the relationship in the cache and returns the newly created relationship. If no relationship
     * was created it returns an empty option (so it means that a cache hit happened, the relationship already exists).
     * @param client is the client (producer or consumer) that is related to a topic or consumergroup.
     * @param relatedObject is a related object, practically a topic or a consumergroup.
     * @return if there was a cache miss, it returns the newly added relationship. If there was a cache hit it returns
     * an empty optional.
     */
    public Optional<ClientRelationship<T, U>> putRelationship(T client, U relatedObject) {
        Set<U> relations = cache.getIfPresent(client);
        if (relations == null) {
            relations = ConcurrentHashMap.newKeySet();
            relations.add(relatedObject);
            cache.put(client, relations);
            return Optional.of(new ClientRelationship<>(client, relatedObject));
        } else if (relations.add(relatedObject)) {
            return Optional.of(new ClientRelationship<>(client, relatedObject));
        } else {
            return Optional.empty();
        }
    }
}
