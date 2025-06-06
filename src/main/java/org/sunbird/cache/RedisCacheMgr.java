package org.sunbird.cache;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.core.logger.CbExtLogger;

import com.fasterxml.jackson.databind.ObjectMapper;


import javax.annotation.PostConstruct;

@Component
public class RedisCacheMgr {

    private static int cache_ttl = 84600;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

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
        try {
            String data = objectMapper.writeValueAsString(object);
            redisTemplate.opsForValue().set(key, data, ttl, TimeUnit.SECONDS);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error(e);
        }
    }
    public void putCache(String key, Object object) {
        putCache(key,object,cache_ttl);
    }
    public void putInQuestionCache(String key, Object object) {
        try {
            String data = objectMapper.writeValueAsString(object);
            redisTemplate.opsForValue().set(Constants.REDIS_COMMON_KEY + key, data, questions_cache_ttl, TimeUnit.SECONDS);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error(e);
        }
    }
    public void putStringInCache(String key, String value,int ttl) {
        try {
            redisTemplate.opsForValue().set(Constants.REDIS_COMMON_KEY + key, value, ttl, TimeUnit.SECONDS);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public void putStringInCache(String key, String value) {
        putStringInCache(key, value, cache_ttl);
    }

    public boolean deleteKeyByName(String key) {
        try {
        	redisTemplate.delete(Constants.REDIS_COMMON_KEY + key);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is deleted from redis");
            return true;
        } catch (Exception e) {
            logger.error(e);
            return false;
        }
    }

    public boolean deleteAllCBExtKey() {
        try {
            String keyPattern = Constants.REDIS_COMMON_KEY + "*";
            Set<String> keys = redisTemplate.keys(keyPattern);
            if (keys != null && !keys.isEmpty()) {
                redisTemplate.delete(keys);
            }
            logger.info("All Keys starts with " + Constants.REDIS_COMMON_KEY + " is deleted from redis");
            return true;
        } catch (Exception e) {
            logger.error(e);
            return false;
        }
    }

    public String getCache(String key) {
        try {
            return redisTemplate.opsForValue().get(Constants.REDIS_COMMON_KEY + key);
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

    public List<String> mget(List<String> fields) {
        try {
            List<String> updatedKeys = new ArrayList<>();
            for (String field : fields) {
                updatedKeys.add(Constants.REDIS_COMMON_KEY + Constants.QUESTION_ID + field);
            }
            return redisTemplate.opsForValue().multiGet(updatedKeys);
        } catch (Exception e) {
            logger.error(e);
        }
        return null;
    }

    public Set<String> getAllKeyNames() {
        try {
            String keyPattern = Constants.REDIS_COMMON_KEY + "*";
            return redisTemplate.keys(keyPattern);
        } catch (Exception e) {
            logger.error(e);
            return Collections.emptySet();
        }
    }

    public List<Map<String, Object>> getAllKeysAndValues() {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        try {
            Set<String> keys = redisTemplate.keys(Constants.REDIS_COMMON_KEY + "*");
            if (keys != null && !keys.isEmpty()) {
                Map<String, Object> res = new HashMap<>();
                List<String> values = redisTemplate.opsForValue().multiGet(keys);

                if (values != null) {
                    Iterator<String> keyIterator = keys.iterator();
                    Iterator<String> valueIterator = values.iterator();

                    while (keyIterator.hasNext() && valueIterator.hasNext()) {
                        res.put(keyIterator.next(), valueIterator.next());
                    }
                }
                result.add(res);
            }
            return result;
        } catch (Exception e) {
            logger.error(e);
            return Collections.emptyList();
        }
    }

    public List<String> hget(String key, int index, String... fields) {
        try {
            return redisTemplate.opsForHash().multiGet(key, Arrays.asList(fields))
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

    public String getCache(String key, Integer index) {
        try {
            return redisTemplate.opsForValue().get(key);
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

    public String getCacheFromDataRedish(String key, Integer index) {
        try {
            return redisTemplate.opsForValue().get(key);
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

    public String getHashedCacheFromDataRedis(String key, Integer index, String field) {
        try{
            Object value = redisTemplate.opsForHash().get(key, field);
            return value != null ? value.toString() : null;
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

    public String getContentFromCache(String key) {
        try {
            return redisTemplate.opsForValue().get(key);
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

    public boolean keyExists(String key) {
        try {
            Boolean exists = redisTemplate.hasKey(Constants.REDIS_COMMON_KEY + key);
            return exists != null && exists;
        } catch (Exception e) {
            logger.error("An Error Occurred while fetching value from Redis", e);
            return false;
        }
    }

    public boolean valueExists(String key, String value) {
        try {
            Boolean isMember = redisTemplate.opsForSet().isMember(Constants.REDIS_COMMON_KEY + key, value);
            return isMember != null && isMember;
        } catch (Exception e) {
            logger.error("An Error Occurred while fetching value from Redis", e);
            return false;
        }
    }

    public void putCacheAsStringArray(String key, String[] values, Integer ttl) {
        try{
            int actualTtl = (ttl == null) ? (int) cache_ttl : ttl;
            redisTemplate.opsForSet().add(Constants.REDIS_COMMON_KEY + key, values);
            redisTemplate.expire(Constants.REDIS_COMMON_KEY + key, actualTtl, TimeUnit.SECONDS);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error("An error occurred while saving data into Redis",e);
        }
    }

    public Set<String> getSetFromCacheAsCommaSeparated(String key) {
        try {
            return redisTemplate.opsForSet().members(key);
        } catch (Exception e) {
            logger.error("Failed to fetch Set from Redis cache: ", e);
            return null;
        }
    }

    public void putInBasicProfileCache(String key, Object object) {
        try {
            String data = objectMapper.writeValueAsString(object);
            redisTemplate.opsForValue().set(Constants.BASIC_PROFILE_KEY + key, data, cache_ttl, TimeUnit.SECONDS);
            logger.debug("Cache_key_value " + Constants.BASIC_PROFILE_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error(e);
        }
    }
}
