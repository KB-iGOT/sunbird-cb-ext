package org.sunbird.core.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.core.logger.CbExtLogger;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
@EnableCaching
public class RedisConfig {

	private final CbExtLogger logger = new CbExtLogger(getClass().getName());

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

	/**
	 * Pool for the per-user cache hash (userprofile:{userId}) owned by sunbird-lms-service. Absent
	 * (null bean) when user_profile_redis_host is blank; callers must treat it as optional.
	 */
	@Bean(name = "userProfileJedisPool")
	public JedisPool userProfileJedisPool() {
		String host = cbProperties.getUserProfileRedisHost();
		if (host == null || host.trim().isEmpty()) {
			return null;
		}
		final JedisPoolConfig poolConfig = buildPoolConfig();
		int port = cbProperties.getUserProfileRedisPort();
		int index = cbProperties.getUserProfileRedisIndex();
		int timeout = 2000;
		if (cbProperties.isUserProfileRedisPasswordEnabled()) {
			String user = cbProperties.getUserProfileRedisUsername();
			String password = cbProperties.getUserProfileRedisPassword();
			if (user != null && !user.trim().isEmpty()) {
				// Jedis 2.9 (Spring Boot 2.0.x) only supports AUTH <password>; a Redis ACL user
				// cannot be honoured here. Fail loudly instead of authenticating as the wrong user.
				logger.warn("user_profile_redis_username is set but this Jedis version has no ACL user "
						+ "support; user-profile cache eviction disabled in cb-ext");
				return null;
			}
			return new JedisPool(poolConfig, host.trim(), port, timeout, password, index);
		}
		return new JedisPool(poolConfig, host.trim(), port, timeout, null, index);
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
