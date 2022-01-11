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

import com.alibaba.csp.sentinel.datasource.AbstractWritableDataSource;
import com.alibaba.csp.sentinel.datasource.DataSourceHolder;
import com.alibaba.csp.sentinel.datasource.converter.SentinelConverter;
import com.alibaba.csp.sentinel.datasource.redis.config.RedisConnectionConfig;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

/**
 *
 * @author Jiajiangnan
 */
public class RedisWritableDataSource<T> extends AbstractWritableDataSource<T> {


    private final RedisConnectionConfig connectionConfig;
    private final String ruleKey;
    private final String channel;

    private final DataSourceHolder dataSourceHolder;

    private final SentinelConverter<String, T> converter;

    private AbstractRedisClient redisClient;


    public RedisWritableDataSource(final RedisConnectionConfig connectionConfig, String ruleKey, String channel, final DataSourceHolder dataSourceHolder) {
        super(dataSourceHolder);
        this.connectionConfig = connectionConfig;
        this.ruleKey = ruleKey;
        this.channel = channel;

        this.converter = dataSourceHolder.getConverter();
        this.dataSourceHolder = dataSourceHolder;

        init();
    }

    private void init() {
        initRedisClient();
    }

    private void initRedisClient() {
        redisClient = dataSourceHolder.getDataSourceClient() == null ? null : (AbstractRedisClient) dataSourceHolder.getDataSourceClient();
    }

    @Override
    public void write(T value) throws Exception {
        String content = converter.fromSentinel(value);

        if (redisClient instanceof RedisClient) {
            RedisClient client = (RedisClient) redisClient;
            RedisCommands<String, String> stringRedisCommands = client.connect().sync();
            stringRedisCommands.set(ruleKey, content);
        }
        if(redisClient instanceof RedisClusterClient) {
            RedisClusterClient client = (RedisClusterClient) redisClient;
            RedisAdvancedClusterCommands<String, String> stringRedisCommands = client.connect().sync();
            stringRedisCommands.set(ruleKey, content);
        }
    }

    @Override
    public void close() {
        // Nothing to do
    }
}
