package org.sunbird.core.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.sunbird.common.util.CbExtServerProperties;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
@EnableCaching
public class RedisConfig {

	@Autowired
	CbExtServerProperties cbProperties;

	@Bean
	public JedisPool jedisPool() {
		final JedisPoolConfig poolConfig = buildPoolConfig();
		JedisPool jedisPool = new JedisPool(poolConfig, cbProperties.getRedisHostName(),
				Integer.parseInt(cbProperties.getRedisPort()));
		return jedisPool;
	}

	@Bean
	public JedisPool jedisDataPopulationPool() {
		final JedisPoolConfig poolConfig = buildPoolConfig();
		JedisPool jedisPool = new JedisPool(poolConfig, cbProperties.getRedisDataHostName(),
				Integer.parseInt(cbProperties.getRedisDataPort()));
		return jedisPool;
	}

	@Bean
	public JedisPool jedisUserInsightsPool() {
		final JedisPoolConfig poolConfig = buildPoolConfig();
		return new JedisPool(poolConfig, cbProperties.getRedisUserInsightsHostName(),
				Integer.parseInt(cbProperties.getRedisUserInsightsPort()));
	}

	private JedisPoolConfig buildPoolConfig() {
		final JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxIdle(cbProperties.getRedisPoolMaxIdle());
		poolConfig.setMaxTotal(cbProperties.getRedisPoolMaxTotal());
		poolConfig.setMinIdle(cbProperties.getRedisPoolMinIdle());
		poolConfig.setTestOnBorrow(cbProperties.isRedisPoolTestOnBorrow());
		poolConfig.setTestOnReturn(cbProperties.isRedisPoolTestOnReturn());
		poolConfig.setTestWhileIdle(cbProperties.isRedisPoolTestWhileIdle());
		poolConfig.setMinEvictableIdleTimeMillis(cbProperties.getRedisPoolMinEvictableIdleTimeMs());
		poolConfig.setTimeBetweenEvictionRunsMillis(cbProperties.getRedisPoolTimeBetweenEvictionRunsMs());
		poolConfig.setNumTestsPerEvictionRun(cbProperties.getRedisPoolNumTestsPerEvictionRun());
		poolConfig.setBlockWhenExhausted(cbProperties.isRedisPoolBlockWhenExhausted());
		return poolConfig;
	}
}
