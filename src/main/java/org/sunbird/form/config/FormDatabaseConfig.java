package org.sunbird.form.config;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Read-only access to the cb-ext-form-service database ("form"). Form submissions moved
 * there from the fs-forms-data-alias-v2 ES index; the BP and peer validation reports read
 * them through FormSubmissionReadRepository.
 */
@Configuration
public class FormDatabaseConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(FormDatabaseConfig.class);

    @Bean(name = "formDataSource")
    @ConfigurationProperties(prefix = "form.datasource")
    public DataSource formDataSource() {
        DataSource dataSource = DataSourceBuilder.create().build();
        LOGGER.info("Form DataSource created successfully.");
        return dataSource;
    }

    @Bean(name = "formJdbcTemplate")
    public NamedParameterJdbcTemplate formJdbcTemplate(@Qualifier("formDataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }
}
