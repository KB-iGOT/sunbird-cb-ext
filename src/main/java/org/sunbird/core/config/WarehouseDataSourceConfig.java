package org.sunbird.core.config;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableJpaRepositories(
    basePackages = "org.sunbird.org.warehouse.repository",
    entityManagerFactoryRef = "warehouseEntityManagerFactory",
    transactionManagerRef = "warehouseTransactionManager"
)
public class WarehouseDataSourceConfig {

    @Bean(name = "warehouseDataSource")
    @ConfigurationProperties(prefix = "warehouse.datasource")
    public DataSource warehouseDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "warehouseEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean warehouseEntityManagerFactory(
            EntityManagerFactoryBuilder builder,
            @Qualifier("warehouseDataSource") DataSource dataSource) {
        return builder
                .dataSource(dataSource)
                .packages("org.sunbird.org.warehouse.entity")
                .persistenceUnit("warehouse")
                .build();
    }

    @Bean(name = "warehouseTransactionManager")
    public PlatformTransactionManager warehouseTransactionManager(
            @Qualifier("warehouseEntityManagerFactory") EntityManagerFactory emf) {
        return new JpaTransactionManager(emf);
    }
}
