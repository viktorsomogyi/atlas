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

import com.google.common.collect.Sets;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.kafka.hook.entities.AtlasKafkaClientEntity;
import org.apache.atlas.kafka.hook.entities.ClientLineageEntity;
import org.apache.atlas.kafka.hook.entities.ConsumerGroupEntity;
import org.apache.atlas.kafka.hook.entities.TopicEntity;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class KafkaHookNotification extends AtlasHook implements AtlasConnector {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaHookNotification.class.getName());

    public static final String CONF_REALM_NAME = "atlas.realm.name";
    public static final String REALM_SEPARATOR = "@";
    private static final String ATTRIBUTE_QUALIFIED_NAME = "qualifiedName";

    private static final String REALM;


    static {
        REALM = atlasProperties.getString(CONF_REALM_NAME, DEFAULT_CLUSTER_NAME);
    }

    public KafkaHookNotification() {

    }

    @Override
    public void create(List<TopicEntity> topics) {
        notifyAtlas(topics.stream().map(topic -> getCreateHookNotification(topic.toAtlasEntity(), getUser())).collect(Collectors.toList()));
    }

    @Override
    public void delete(List<TopicEntity> topics) {
        notifyAtlas(topics.stream().map(topic -> getDeleteHookNotification(topic.toAtlasEntity(), getUser())).collect(Collectors.toList()));
    }

    @Override
    public void create(AtlasKafkaClientEntity client) {
        notifyAtlas(Collections.singletonList(getCreateHookNotification(client.toAtlasEntity(), getUser())));
    }

    @Override
    public void update(AtlasKafkaClientEntity client) {
        notifyAtlas(Collections.singletonList(getPartialUpdateHookNotification(client.toAtlasEntity(), getUser())));
    }

    @Override
    public void create(ClientLineageEntity clientLineage) {
        notifyAtlas(Collections.singletonList(getCreateHookNotification(clientLineage.toAtlasEntity(), getUser())));
    }

    @Override
    public void update(ClientLineageEntity clientLineage) {
        notifyAtlas(Collections.singletonList(getPartialUpdateHookNotification(clientLineage.toAtlasEntity(), getUser())));
    }

    @Override
    public void update(List<TopicEntity> topics) {
        notifyAtlas(topics.stream().map(topic -> createPartialUpdateHookNotification(topic.toAtlasEntity()))
                .collect(Collectors.toList()));
    }

    @Override
    public void create(ConsumerGroupEntity consumerGroup) {
        notifyAtlas(Collections.singletonList(getCreateHookNotification(consumerGroup.toAtlasEntity(), getUser())));
    }

    @Override
    public void close() {
    }

    public HookNotification getCreateHookNotification(AtlasEntity entity, String user) {
        return new HookNotification.EntityCreateRequestV2(user, new AtlasEntity.AtlasEntitiesWithExtInfo(entity));
    }

    public HookNotification getPartialUpdateHookNotification(AtlasEntity entity, String user) {
        AtlasObjectId objectId = new AtlasObjectId(entity.getTypeName(), ATTRIBUTE_QUALIFIED_NAME,
                entity.getAttribute(ATTRIBUTE_QUALIFIED_NAME));
        return new HookNotification.EntityPartialUpdateRequestV2(user, objectId, new AtlasEntity.AtlasEntityWithExtInfo(entity));
    }

    public HookNotification getDeleteHookNotification(AtlasEntity entity, String user) {
        AtlasObjectId objectId = new AtlasObjectId(entity.getTypeName(), ATTRIBUTE_QUALIFIED_NAME,
                entity.getAttribute(ATTRIBUTE_QUALIFIED_NAME));
        return new HookNotification.EntityDeleteRequestV2(user, Collections.singletonList(objectId));
    }

    public HookNotification createPartialUpdateHookNotification(AtlasEntity entity) {
        AtlasObjectId objectId = new AtlasObjectId(entity.getTypeName(), ATTRIBUTE_QUALIFIED_NAME,
            entity.getAttribute(ATTRIBUTE_QUALIFIED_NAME));
        entity.removeAttribute(ATTRIBUTE_QUALIFIED_NAME);
        return new HookNotification.EntityPartialUpdateRequestV2(getUser(), objectId,
            new AtlasEntity.AtlasEntityWithExtInfo(entity));
    }

    private void notifyAtlas(List<HookNotification> notifications) {
        try {
            final UserGroupInformation ugi = getUgiFromUserName(getUser());
            super.notifyEntities(notifications, ugi);
        } catch (IOException e) {
            LOG.error("Cannot create entities, it failed with the following error", e);
        }
    }

    private UserGroupInformation getUgiFromUserName(String userName) throws IOException {
        String userPrincipal = userName.contains(REALM_SEPARATOR) ? userName : userName + "@" + REALM;
        Subject userSubject = new Subject(false, Sets.newHashSet(
                new KerberosPrincipal(userPrincipal)), new HashSet<>(), new HashSet<>());
        return UserGroupInformation.getUGIFromSubject(userSubject);
    }
}
