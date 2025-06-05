package org.sunbird.portal;

import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.util.Timeout;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@ComponentScan(basePackages = "org.sunbird")
@EntityScan("org.sunbird")
@SpringBootApplication
@EnableAutoConfiguration
public class SbCbExtApplication {
	/**
	 * Runs The application
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		SpringApplication.run(SbCbExtApplication.class, args);
	}

	/**
	 * Initializes the rest template
	 * 
	 * @return
	 * @throws Exception
	 */

	@Bean
	public RestTemplate restTemplate() throws Exception {
		return new RestTemplate(getClientHttpRequestFactory());
	}

	private ClientHttpRequestFactory getClientHttpRequestFactory() {
		int timeout = 45000;
		org.apache.hc.client5.http.config.RequestConfig config = org.apache.hc.client5.http.config.RequestConfig.custom()
				.setResponseTimeout(Timeout.ofMilliseconds(timeout))
				.build();
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		cm.setMaxTotal(2000);
		cm.setDefaultMaxPerRoute(500);
		org.apache.hc.client5.http.impl.classic.CloseableHttpClient client = HttpClients.custom()
				.setDefaultRequestConfig(config)
				.setConnectionManager(cm)
				.build();
		return new HttpComponentsClientHttpRequestFactory(client);
	}
}
