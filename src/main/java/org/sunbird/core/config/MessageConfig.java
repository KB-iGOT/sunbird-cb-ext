package org.sunbird.core.config;

import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.ResourceBundleMessageSource;
import org.sunbird.common.util.Constants;

@Configuration
public class MessageConfig {

    @Bean
    public MessageSource messageSource() {
        ResourceBundleMessageSource messageSource = new ResourceBundleMessageSource();
        messageSource.setBasename(Constants.MESSAGE_SOURCE_BASENAME);
        messageSource.setDefaultEncoding(Constants.UTF_8);
        return messageSource;
    }


}
