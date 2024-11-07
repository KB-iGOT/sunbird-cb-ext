package org.sunbird.bpreports.postgres.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

@Configuration
@EnableTransactionManagement
@PropertySource({"classpath:application.properties"})
@EnableJpaRepositories(
        basePackages = "org.sunbird.bpreports.postgres.repository",  // Package for the second DB repositories
        entityManagerFactoryRef = "wingspanEntityManagerFactory",
        transactionManagerRef = "wingspanTransactionManager"
)
public class WingspanDatabaseConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(WingspanDatabaseConfig.class);

    // Define the DataSource for the Wingspan database
    @Bean(name = "wingspanDataSource")
    @ConfigurationProperties(prefix = "wingspan.datasource")
    public DataSource wingspanDataSource() {
        DataSource dataSource = DataSourceBuilder.create().build();
        LOGGER.info("Wingspan DataSource created successfully.");
        return dataSource;
    }

    // Define the EntityManagerFactory for the Wingspan database
    @Bean(name = "wingspanEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean wingspanEntityManagerFactory(
            EntityManagerFactoryBuilder builder,
            @Qualifier("wingspanDataSource") DataSource dataSource) {
        LOGGER.info("Configuring Wingspan EntityManagerFactory");
        return builder
                .dataSource(dataSource)
                .packages("org.sunbird.bpreports.postgres.entity")  // Specify the package for Wingspan DB entities
                .persistenceUnit("wingspan")  // Define a persistence unit name
                .build();
    }

    // Define the TransactionManager for the Wingspan database
    @Bean(name = "wingspanTransactionManager")
    public PlatformTransactionManager wingspanTransactionManager(
            @Qualifier("wingspanEntityManagerFactory") EntityManagerFactory wingspanEntityManagerFactory) {
        LOGGER.info("Configuring Wingspan TransactionManager");
        return new JpaTransactionManager(wingspanEntityManagerFactory);
    }
}