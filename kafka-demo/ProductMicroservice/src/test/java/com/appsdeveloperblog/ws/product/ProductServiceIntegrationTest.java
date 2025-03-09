package com.appsdeveloperblog.ws.product;

import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@DirtiesContext // Each Test will start with a clean context
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ActiveProfiles("test") //application-test.properties
@EmbeddedKafka(partitions = 3, count = 3, controlledShutdown = true)
@SpringBootTest("properties={spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}}")
public class ProductServiceIntegrationTest {
}
