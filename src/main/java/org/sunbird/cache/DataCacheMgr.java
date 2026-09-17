package org.sunbird.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.sunbird.common.util.CbExtServerProperties;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

@Component
public class DataCacheMgr {

    @Autowired
    private CbExtServerProperties cbExtServerProperties;

    private Map<String, String> strCacheMap = new HashMap<String, String>();

    private Map<String, Object> objCacheMap = new HashMap<String, Object>();

    private Cache<String, Map<String, Object>> contentCacheMap;

    @PostConstruct
    public void init() {
        contentCacheMap = Caffeine.newBuilder()
                .expireAfterWrite(cbExtServerProperties.getContentInMemoryCacheTtlSeconds(), TimeUnit.SECONDS)
                .maximumSize(cbExtServerProperties.getContentInMemoryCacheMaxSize())
                .build();
    }

    public void putStringInCache(String key, String value) {
        strCacheMap.put(key, value);
    }

    public void putObjectInCache(String key, Object value) {
        objCacheMap.put(key, value);
    }

    public String getStringFromCache(String key) {
        if (strCacheMap.containsKey(key)) {
            return strCacheMap.get(key);
        }
        return "";
    }

    public Object getObjectFromCache(String key) {
        if (objCacheMap.containsKey(key)) {
            return objCacheMap.get(key);
        }
        return null;
    }

    public void putContentInCache(String key, Map<String, Object> value) {
        contentCacheMap.put(key, value);
    }

    public Map<String, Object> getContentFromCache(String key) {
        return contentCacheMap.getIfPresent(key);
    }
}
