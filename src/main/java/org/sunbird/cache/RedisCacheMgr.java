package org.sunbird.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.core.logger.CbExtLogger;

import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import javax.annotation.PostConstruct;

@Component
public class RedisCacheMgr {

    private static int cache_ttl = 84600;

    @Autowired
    private JedisPool jedisPool;

    @Autowired
    private JedisPool jedisDataPopulationPool;

    @Autowired
    private JedisPool jedisUserInsightsPool;

    @Autowired
    CbExtServerProperties cbExtServerProperties;

    private CbExtLogger logger = new CbExtLogger(getClass().getName());
    
    ObjectMapper objectMapper = new ObjectMapper();

    private static int questions_cache_ttl = 84600;

    @PostConstruct
    public void postConstruct() {
        this.questions_cache_ttl = cbExtServerProperties.getRedisQuestionsReadTimeOut().intValue();
        if (!StringUtils.isEmpty(cbExtServerProperties.getRedisTimeout())) {
            cache_ttl = Integer.parseInt(cbExtServerProperties.getRedisTimeout());
        }
    }
    public void putCache(String key, Object object, int ttl) {
        try (Jedis jedis = jedisPool.getResource()) {
            String data = objectMapper.writeValueAsString(object);
            jedis.set(Constants.REDIS_COMMON_KEY + key, data);
            jedis.expire(Constants.REDIS_COMMON_KEY + key, ttl);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error(e);
        }
    }
    public void putCache(String key, Object object) {
        putCache(key,object,cache_ttl);
    }
    public void putInQuestionCache(String key, Object object) {
        try (Jedis jedis = jedisPool.getResource()) {
            String data = objectMapper.writeValueAsString(object);
            jedis.set(Constants.REDIS_COMMON_KEY + key, data);
            jedis.expire(Constants.REDIS_COMMON_KEY + key, questions_cache_ttl);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error(e);
        }
    }
    public void putStringInCache(String key, String value,int ttl) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(Constants.REDIS_COMMON_KEY + key, value);
            jedis.expire(Constants.REDIS_COMMON_KEY + key, ttl);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public void putStringInCache(String key, String value) {
        putStringInCache(key, value, cache_ttl);
    }

    public boolean deleteKeyByName(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
        	jedis.del(Constants.REDIS_COMMON_KEY + key);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is deleted from redis");
            return true;
        } catch (Exception e) {
            logger.error(e);
            return false;
        }
    }

    public boolean deleteAllCBExtKey() {
        try (Jedis jedis = jedisPool.getResource()) {
            String keyPattern = Constants.REDIS_COMMON_KEY + "*";
            Set<String> keys = jedis.keys(keyPattern);
            for (String key : keys) {
            	jedis.del(key);
            }
            logger.info("All Keys starts with " + Constants.REDIS_COMMON_KEY + " is deleted from redis");
            return true;
        } catch (Exception e) {
            logger.error(e);
            return false;
        }
    }

    public String getCache(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(Constants.REDIS_COMMON_KEY + key);
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

    public List<String> mget(List<String> fields) {
        try (Jedis jedis = jedisPool.getResource()) {
        	String[] updatedKeys = new String[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
            	updatedKeys[i] = Constants.REDIS_COMMON_KEY + Constants.QUESTION_ID + fields.get(i);
            }
            return jedis.mget(updatedKeys);
        } catch (Exception e) {
            logger.error(e);
        }
        return null;
    }

    public Set<String> getAllKeyNames() {
        try (Jedis jedis = jedisPool.getResource()) {
            String keyPattern = Constants.REDIS_COMMON_KEY + "*";
            return jedis.keys(keyPattern);
        } catch (Exception e) {
            logger.error(e);
            return Collections.emptySet();
        }
    }

    public List<Map<String, Object>> getAllKeysAndValues() {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        try (Jedis jedis = jedisPool.getResource()) {
            String keyPattern = Constants.REDIS_COMMON_KEY + "*";
            Map<String, Object> res = new HashMap<>();
            Set<String> keys = jedis.keys(keyPattern);
            if (!keys.isEmpty()) {
                for (String key : keys) {
                    Object entries;
                    entries = jedis.get(key);
                    res.put(key, entries);
                }
                result.add(res);
            }
        } catch (Exception e) {
            logger.error(e);
            return Collections.emptyList();
        }
        return result;
    }
    
    public List<String> hget(String key, int index, String... fields) {
        try (Jedis jedis = jedisDataPopulationPool.getResource()) {
            jedis.select(index);
            return jedis.hmget(key, fields);
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

    public String getCache(String key, Integer index) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (index != null) {
                jedis.select(index);
            }
            return jedis.get(key);
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

    public String getCacheFromDataRedish(String key, Integer index) {
        try (Jedis jedis = jedisDataPopulationPool.getResource()) {
            if (index != null) {
                jedis.select(index);
            }
            return jedis.get(key);
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

    public String getHashedCacheFromDataRedis(String key, Integer index, String field) {
        try (Jedis jedis = jedisDataPopulationPool.getResource()) {
            if (index != null) {
                jedis.select(index);
            }
            return jedis.hget(key,field);
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

    public Map<String, String> getAllHashFieldsFromDataRedis(String key, Integer index) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (index != null) {
                jedis.select(index);
            }
            return jedis.hgetAll(key);
        } catch (Exception e) {
            logger.error(e);
            return Collections.emptyMap();
        }
    }

    public List<String> getListFromDataRedis(String key, Integer index, long start, long end) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (index != null) {
                jedis.select(index);
            }
            return jedis.lrange(key, start, end);
        } catch (Exception e) {
            logger.error(e);
            return Collections.emptyList();
        }
    }

    public String getContentFromCache(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

    public boolean keyExists(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(Constants.REDIS_COMMON_KEY + key);
        } catch (Exception e) {
            logger.error("An Error Occurred while fetching value from Redis", e);
            return false;
        }
    }

    public boolean valueExists(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.sismember(Constants.REDIS_COMMON_KEY + key, value);
        } catch (Exception e) {
            logger.error("An Error Occurred while fetching value from Redis", e);
            return false;
        }
    }

    public void putCacheAsStringArray(String key, String[] values, Integer ttl) {
        try (Jedis jedis = jedisPool.getResource()) {
            if(null == ttl)
                ttl = cache_ttl;
            jedis.sadd(Constants.REDIS_COMMON_KEY + key, values);
            jedis.expire(Constants.REDIS_COMMON_KEY + key, ttl);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error("An error occurred while saving data into Redis",e);
        }
    }

    public Set<String> getSetFromCacheAsCommaSeparated(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.select(10);
            return jedis.smembers(key);
        } catch (Exception e) {
            logger.error("Failed to fetch Set from Redis cache: ", e);
            return null;
        }
    }

    public void putInBasicProfileCache(String key, Object object, int ttl) {
        try (Jedis jedis = jedisPool.getResource()) {
            String data = objectMapper.writeValueAsString(object);
            jedis.set(Constants.BASIC_PROFILE_KEY + key, data);
            jedis.expire(Constants.BASIC_PROFILE_KEY + key, ttl);
            logger.debug("Cache_key_value " + Constants.BASIC_PROFILE_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public boolean deleteKeyByNameV2(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key);
            logger.debug("Cache_key_value " + key + " is deleted from redis");
            return true;
        } catch (Exception e) {
            logger.error(e);
            return false;
        }
    }

    /**
     * Serializes and stores an object in the dedicated UserInsights Redis instance under the given key.
     *
     * <p>Uses a raw key (no {@code CB_EXT_} prefix) because this is a purpose-specific Redis instance
     * rather than the shared primary cache. The entry is set with an explicit TTL so stale user-level
     * stats are automatically evicted without manual intervention.</p>
     *
     * @param key    the raw Redis key (e.g. {@code userInsights_{userId}})
     * @param object the object to serialize and cache; must be Jackson-serializable
     * @param ttl    time-to-live in seconds for the cached entry
     * @param index  the Redis database index to select before writing
     */
    public void putCacheToUserInsightsRedis(String key, Object object, int ttl, int index) {
        try (Jedis jedis = jedisUserInsightsPool.getResource()) {
            jedis.select(index);
            String data = objectMapper.writeValueAsString(object);
            jedis.set(key, data);
            jedis.expire(key, ttl);
            logger.debug("[UserInsights Redis] Saved key: " + key);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * Retrieves a raw JSON string from the dedicated UserInsights Redis instance for the given key.
     *
     * <p>Selects the specified database index before reading, matching the index used during the write.
     * Returns {@code null} on a cache miss or any connectivity failure — callers are expected to handle
     * a {@code null} return as a signal to fall back to the primary data source (Postgres).</p>
     *
     * @param key   the raw Redis key to look up (e.g. {@code userInsights_{userId}})
     * @param index the Redis database index to select before reading
     * @return the cached JSON string, or {@code null} if not found or an error occurs
     */
    public String getCacheFromUserInsightsRedis(String key, int index) {
        try (Jedis jedis = jedisUserInsightsPool.getResource()) {
            jedis.select(index);
            return jedis.get(key);
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

}
