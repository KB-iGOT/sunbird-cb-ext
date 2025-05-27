package org.sunbird.core.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import java.net.InetSocketAddress;
import java.util.Objects;


@Configuration
@ConfigurationProperties("spring.data.cassandra.sb")
@EnableCassandraRepositories(basePackages = { "org.sunbird.assessment.repo" }, cassandraTemplateRef = "sunbirdTemplate")
public class SunbirdConfig extends CassandraConfig {

	private Logger logger = LoggerFactory.getLogger(SunbirdConfig.class);

	@Value("${spring.data.cassandra.sb.username}")
	private String sunbirdUser;

	@Value("${spring.data.cassandra.sb.password}")
	private String sunbirdPassword;

	@Value("${spring.data.cassandra.sb.local-datacenter}")
	private String localDatacenter;

	@Primary
	@Bean(name = "sunbirdTemplate")
	public CassandraAdminTemplate sunbirdCassandraTemplate(@Autowired CqlSession cqlSession) {
		try {
			return new CassandraAdminTemplate(cqlSession, cassandraConverter());
		} catch (Exception e) {
			logger.error("Error creating cassandra template", e);
			throw new RuntimeException("Failed to create cassandra template", e);
		}
	}

	@Primary
	@Bean(name = "sunbirdSession")
	public CqlSession cqlSession() {
		logger.info("Creating CqlSession for keyspace: {}", getKeyspaceName());

		CqlSessionBuilder builder = CqlSession.builder();

		String[] contactPoints = getContactPoints().split(",");

		for (String contactPoint : contactPoints) {
			builder.addContactPoint(new InetSocketAddress(contactPoint.trim(), getPort()));
		}

		builder.withLocalDatacenter(Objects.requireNonNull(getLocalDataCenter()))
				.withKeyspace(getKeyspaceName());

		if (!sunbirdUser.isEmpty() && !sunbirdPassword.isEmpty()) {
			builder.withAuthCredentials(sunbirdUser, sunbirdPassword);
		}

		return builder.build();
	}
}