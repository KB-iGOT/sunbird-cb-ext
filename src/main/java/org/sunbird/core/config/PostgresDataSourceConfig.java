package org.sunbird.core.config;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableJpaRepositories(
        basePackages = "org.sunbird.org.repository",
        entityManagerFactoryRef = "sunbirdEntityManagerFactory",
        transactionManagerRef = "sunbirdTransactionManager"
)
public class PostgresDataSourceConfig {
    @Bean
    @Primary
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource sunbirdDataSource() {
        return DataSourceBuilder.create().build();
    }

    // Define the EntityManagerFactory for the Wingspan database
    @Primary
    @Bean(name = "sunbirdEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean sunbirdEntityManagerFactory(
            EntityManagerFactoryBuilder builder,
            @Qualifier("sunbirdDataSource") DataSource dataSource) {
        return builder
                .dataSource(dataSource)
                .packages("org.sunbird.org")
                .persistenceUnit("sunbird")
                .build();
    }

    // Define the TransactionManager for the sunbird database
    @Primary
    @Bean(name = "sunbirdTransactionManager")
    public PlatformTransactionManager sunbirdTransactionManager(
            @Qualifier("sunbirdEntityManagerFactory") EntityManagerFactory sunbirdEntityManagerFactory) {
        return new JpaTransactionManager(sunbirdEntityManagerFactory);
    }
}
