package com.alibaba.csp.sentinel.datasource.redis.config;

import com.alibaba.csp.sentinel.log.RecordLog;
import com.alibaba.csp.sentinel.util.StringUtil;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A factory to create RedisClient By RedisConnectionConfig
 *
 * @author: Jia Jiangnan
 */
public class RedisFactory {

    public final static AbstractRedisClient createRedisClient(RedisConnectionConfig connectionConfig) {
        if (connectionConfig.getRedisClusters().size() == 0) {
            return getSingleRedisClient(connectionConfig);
        } else {
            return getRedisClusterClient(connectionConfig);
        }
    }

    /**
     * Build RedisClient of single-mode or sentinel-mode fromm {@code RedisConnectionConfig}.
     *
     * @return a new {@link RedisClient}
     */
    public final static RedisClient getSingleRedisClient(RedisConnectionConfig connectionConfig) {
        if (connectionConfig.getRedisSentinels().size() == 0) {
            RecordLog.info("[RedisDataSource] Creating stand-alone mode Redis client");
            return getRedisStandaloneClient(connectionConfig);
        } else {
            RecordLog.info("[RedisDataSource] Creating Redis Sentinel mode Redis client");
            return getRedisSentinelClient(connectionConfig);
        }
    }

    /**
     * Build  RedisClient of cluster-mode fromm {@code RedisConnectionConfig}.
     *
     * @return a new {@link RedisClient}
     */
    public final static RedisClusterClient getRedisClusterClient(RedisConnectionConfig connectionConfig) {
        char[] password = connectionConfig.getPassword();
        String clientName = connectionConfig.getClientName();

        //If any uri is successful for connection, the others are not tried anymore
        List<RedisURI> redisUris = new ArrayList<>();
        for (RedisConnectionConfig config : connectionConfig.getRedisClusters()) {
            RedisURI.Builder clusterRedisUriBuilder = RedisURI.builder();
            clusterRedisUriBuilder.withHost(config.getHost())
                    .withPort(config.getPort())
                    .withTimeout(Duration.ofMillis(connectionConfig.getTimeout()));
            //All redis nodes must have same password
            if (password != null) {
                clusterRedisUriBuilder.withPassword(connectionConfig.getPassword());
            }
            redisUris.add(clusterRedisUriBuilder.build());
        }
        return RedisClusterClient.create(redisUris);
    }

    public final static RedisClient getRedisStandaloneClient(RedisConnectionConfig connectionConfig) {
        char[] password = connectionConfig.getPassword();
        String clientName = connectionConfig.getClientName();
        RedisURI.Builder redisUriBuilder = RedisURI.builder();
        redisUriBuilder.withHost(connectionConfig.getHost())
                .withPort(connectionConfig.getPort())
                .withDatabase(connectionConfig.getDatabase())
                .withTimeout(Duration.ofMillis(connectionConfig.getTimeout()));
        if (password != null) {
            redisUriBuilder.withPassword(connectionConfig.getPassword());
        }
        if (StringUtil.isNotEmpty(connectionConfig.getClientName())) {
            redisUriBuilder.withClientName(clientName);
        }
        return RedisClient.create(redisUriBuilder.build());
    }

    public final static RedisClient getRedisSentinelClient(RedisConnectionConfig connectionConfig) {
        char[] password = connectionConfig.getPassword();
        String clientName = connectionConfig.getClientName();
        RedisURI.Builder sentinelRedisUriBuilder = RedisURI.builder();
        for (RedisConnectionConfig config : connectionConfig.getRedisSentinels()) {
            sentinelRedisUriBuilder.withSentinel(config.getHost(), config.getPort());
        }
        if (password != null) {
            sentinelRedisUriBuilder.withPassword(connectionConfig.getPassword());
        }
        if (StringUtil.isNotEmpty(connectionConfig.getClientName())) {
            sentinelRedisUriBuilder.withClientName(clientName);
        }
        sentinelRedisUriBuilder.withSentinelMasterId(connectionConfig.getRedisSentinelMasterId())
                .withTimeout(connectionConfig.getTimeout(), TimeUnit.MILLISECONDS);
        return RedisClient.create(sentinelRedisUriBuilder.build());
    }

}
