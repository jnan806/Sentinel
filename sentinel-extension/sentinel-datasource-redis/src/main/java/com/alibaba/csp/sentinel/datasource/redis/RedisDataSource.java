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

import com.alibaba.csp.sentinel.datasource.DataSourceHolder;
import com.alibaba.csp.sentinel.datasource.DataSourceMode;
import com.alibaba.csp.sentinel.datasource.converter.SentinelConverter;
import com.alibaba.csp.sentinel.datasource.redis.config.RedisConnectionConfig;
import com.alibaba.csp.sentinel.datasource.redis.config.RedisFactory;
import com.alibaba.csp.sentinel.util.AssertUtil;
import io.lettuce.core.AbstractRedisClient;

/**
 *
 *
 * @author Jiajiangnan
 */
public class RedisDataSource<T> extends DataSourceHolder {

    private RedisReadableDataSource<T> readableDataSource;
    private RedisWritableDataSource<T> writableDataSource;

    /**
     * Constructor of {@code RedisDataSource}.
     *
     * @param connectionConfig Redis connection config
     * @param ruleKey          data key in Redis
     * @param channel          channel to subscribe in Redis
     * @param converter     customized data converter, cannot be empty
     */
    public RedisDataSource(RedisConnectionConfig connectionConfig, String ruleKey, String channel, SentinelConverter<String, T> converter) {
        this(connectionConfig, ruleKey, channel, converter, DataSourceMode.READABLE);
    }

    /**
     * Constructor of {@code RedisDataSource}.
     *
     * @param connectionConfig Redis connection config
     * @param ruleKey          data key in Redis
     * @param channel          channel to subscribe in Redis
     * @param converter     customized data converter, cannot be empty
     * @param dataSourceMode     dataSourceMode, cannot be empty
     */
    public RedisDataSource(RedisConnectionConfig connectionConfig, String ruleKey, String channel, SentinelConverter<String, T> converter, DataSourceMode dataSourceMode) {
        super(converter, dataSourceMode);
        AssertUtil.notNull(connectionConfig, "Redis connection config can not be null");
        AssertUtil.notEmpty(ruleKey, "Redis ruleKey can not be empty");
        AssertUtil.notEmpty(channel, "Redis subscribe channel can not be empty");

        AbstractRedisClient redisClient = RedisFactory.createRedisClient(connectionConfig);
        super.setDataSourceClient(redisClient);

        if(DataSourceMode.ALL == dataSourceMode || DataSourceMode.READABLE == dataSourceMode) {
            this.readableDataSource = new RedisReadableDataSource(connectionConfig, ruleKey, channel, this);
        }

        if(DataSourceMode.ALL == dataSourceMode || DataSourceMode.WRITABLE == dataSourceMode) {
            this.writableDataSource = new RedisWritableDataSource(connectionConfig, ruleKey, channel, this);
        }

    }

    public RedisReadableDataSource<T> getReader() {
        return this.readableDataSource;
    }

    public RedisWritableDataSource<T> getWriter() {
        return this.writableDataSource;
    }

    public void close() {

        if(DataSourceMode.ALL == dataSourceMode || DataSourceMode.READABLE == dataSourceMode) {
            this.readableDataSource.close();
        }

        if(DataSourceMode.ALL == dataSourceMode || DataSourceMode.WRITABLE == dataSourceMode) {
            this.writableDataSource.close();
        }

        AbstractRedisClient redisClient = (AbstractRedisClient) this.getDataSourceClient();
        if (redisClient != null) {
            redisClient.shutdown();
        } else {
            redisClient.shutdown();
        }
    }

}
