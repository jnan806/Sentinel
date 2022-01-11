/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.csp.sentinel.datasource.redis;

import com.alibaba.csp.sentinel.datasource.AbstractReadableDataSource;
import com.alibaba.csp.sentinel.datasource.DataSourceHolder;
import com.alibaba.csp.sentinel.datasource.converter.SentinelConverter;
import com.alibaba.csp.sentinel.datasource.redis.config.RedisConnectionConfig;
import com.alibaba.csp.sentinel.log.RecordLog;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

/**
 * <p>
 * A read-only {@code DataSource} with Redis backend.
 * </p>
 * <p>
 * The data source first loads initial rules from a Redis String during initialization.
 * Then the data source subscribe from specific channel. When new rules is published to the channel,
 * the data source will observe the change in realtime and update to memory.
 * </p>
 * <p>
 * Note that for consistency, users should publish the value and save the value to the ruleKey simultaneously
 * like this (using Redis transaction):
 * <pre>
 *  MULTI
 *  SET ruleKey value
 *  PUBLISH channel value
 *  EXEC
 * </pre>
 * </p>
 *
 * @author tiger
 * @author Jiajiangnan
 */
public class RedisReadableDataSource<T> extends AbstractReadableDataSource<String, T> {

    private final RedisConnectionConfig connectionConfig;
    private final String ruleKey;
    private final String channel;

    private final DataSourceHolder dataSourceHolder;
    private final boolean autoRefresh;

    private final SentinelConverter<String, T> converter;

    private AbstractRedisClient redisClient;

    public RedisReadableDataSource(final RedisConnectionConfig connectionConfig, String ruleKey, String channel, final DataSourceHolder dataSourceHolder) {
        this(connectionConfig, ruleKey, channel, dataSourceHolder, true);
    }

    public RedisReadableDataSource(final RedisConnectionConfig connectionConfig, final String ruleKey, final String channel, final DataSourceHolder dataSourceHolder, boolean autoRefresh) {
        super(dataSourceHolder);
        this.connectionConfig = connectionConfig;
        this.ruleKey = ruleKey;
        this.channel = channel;

        this.converter = dataSourceHolder.getConverter();
        this.dataSourceHolder = dataSourceHolder;
        this.autoRefresh = autoRefresh;

        init();
    }

    private void init() {
        initRedisClient();
        loadInitialConfig();
        if(this.autoRefresh) {
            subscribeFromChannel();
        }
    }

    private void initRedisClient() {
        redisClient = dataSourceHolder.getDataSourceClient() == null ? null : (AbstractRedisClient) dataSourceHolder.getDataSourceClient();
    }

    private void loadInitialConfig() {
        try {
            T newValue = loadConfig();
            if (newValue == null) {
                RecordLog.warn("[RedisDataSource] WARN: initial config is null, you may have to check your data source");
            }
            getProperty().updateValue(newValue);
        } catch (Exception ex) {
            RecordLog.warn("[RedisDataSource] Error when loading initial config", ex);
        }
    }

    @Override
    public String readSource() {
        if (this.redisClient == null) {
            throw new IllegalStateException("Redis client or Redis Cluster client has not been initialized or error occurred");
        }

        if (redisClient instanceof RedisClient) {
            RedisClient client = (RedisClient) redisClient;
            RedisCommands<String, String> stringRedisCommands = client.connect().sync();
            return stringRedisCommands.get(ruleKey);
        }
        if(redisClient instanceof RedisClusterClient) {
            RedisClusterClient client = (RedisClusterClient) redisClient;
            RedisAdvancedClusterCommands<String, String> stringRedisCommands = client.connect().sync();
            return stringRedisCommands.get(ruleKey);
        }

        throw new IllegalArgumentException("redisClient is neither RedisClient nor RedisClusterClient");
    }

    @Override
    public void close() {
        // Nothing to do
    }


    private void subscribeFromChannel() {
        RedisPubSubAdapter<String, String> adapterListener = new DelegatingRedisPubSubListener();
        if (redisClient instanceof RedisClient) {
            RedisClient client = (RedisClient) redisClient;
            StatefulRedisPubSubConnection<String, String> pubSubConnection = client.connectPubSub();
            pubSubConnection.addListener(adapterListener);
            RedisPubSubCommands<String, String> sync = pubSubConnection.sync();
            sync.subscribe(channel);
        }
        if(redisClient instanceof RedisClusterClient) {
            RedisClusterClient client = (RedisClusterClient) redisClient;
            StatefulRedisClusterPubSubConnection<String, String> pubSubConnection = client.connectPubSub();
            pubSubConnection.addListener(adapterListener);
            RedisPubSubCommands<String, String> sync = pubSubConnection.sync();
            sync.subscribe(channel);
        }

    }

    private class DelegatingRedisPubSubListener extends RedisPubSubAdapter<String, String> {

        DelegatingRedisPubSubListener() {
        }

        @Override
        public void message(String channel, String message) {
            RecordLog.info("[RedisDataSource] New property value received for channel {}: {}", channel, message);
            getProperty().updateValue(converter.toSentinel(message));
        }
    }

}
