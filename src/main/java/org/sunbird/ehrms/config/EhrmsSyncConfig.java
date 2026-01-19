package org.sunbird.ehrms.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class EhrmsSyncConfig {

    @Value("${ehrms.user.sync.threads}")
    private String ehrmsUserSyncThreads;

    @Bean("ehrmsSyncExecutor")
    public ExecutorService ehrmsSyncExecutor() {
        // Fixed thread pool, isolated from main application threads
        return Executors.newFixedThreadPool(Integer.parseInt(ehrmsUserSyncThreads)); // adjust 8 based on your load tests
    }
}
