package org.sunbird.form.config;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

class FormDatabaseConfigTest {

    @Test
    void createsFormDataSourceAndJdbcTemplate() {
        FormDatabaseConfig config = new FormDatabaseConfig();

        DataSource dataSource = config.formDataSource();
        NamedParameterJdbcTemplate template = config.formJdbcTemplate(dataSource);

        assertNotNull(dataSource);
        assertSame(dataSource, template.getJdbcTemplate().getDataSource());
    }
}
