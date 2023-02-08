package de.turing85;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.file;

import de.turing85.config.ServiceConfig;
import de.turing85.config.IdempotencyConfig;
import java.util.Random;
import javax.enterprise.context.ApplicationScoped;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@ApplicationScoped
public class Route extends RouteBuilder {
  public static final String ROUTE_ID = "file-mover";
  private final CustomJdbcMessageIdRepository idempotentRepository;
  private final Random random;

  public Route(
      @SuppressWarnings("CdiInjectionPointsInspection") DataSource dataSource,
      @SuppressWarnings("CdiInjectionPointsInspection") TransactionManager transactionManager,
      ServiceConfig serviceConfig,
      CamelContext context) {
    idempotentRepository =
        constructRepository(dataSource, serviceConfig, context, transactionManager);
    this.random = new Random();
  }

  private static CustomJdbcMessageIdRepository constructRepository(
      DataSource dataSource,
      ServiceConfig serviceConfig,
      CamelContext context,
      TransactionManager transactionManager) {
    IdempotencyConfig idempotencyConfig = serviceConfig.idempotencyConfig();
    String serviceName = serviceConfig.name();
    final CustomJdbcMessageIdRepository repository =
        new CustomJdbcMessageIdRepository(dataSource, ROUTE_ID, context);
    repository.setTransactionTemplate(new TransactionTemplate(new JtaTransactionManager(transactionManager)));
    repository.setTableName(idempotencyConfig.tableName());
    repository.setLockMaxAgeMillis(idempotencyConfig.lockMaxAge().toMillis());
    repository.setLockKeepAliveIntervalMillis(idempotencyConfig.keepAliveInterval().toMillis());
    repository.setCreateTableIfNotExists(true);
    repository.setCreateString("""
        CREATE TABLE CAMEL_MESSAGEPROCESSED (
            serviceName VARCHAR(255),
            processorName VARCHAR(255),
            messageId VARCHAR(100),
            createdAt TIMESTAMP,
            done BOOLEAN DEFAULT false,
            PRIMARY KEY (serviceName, processorName, messageId)
        )
        """);
    repository.setInsertString("""
        INSERT INTO CAMEL_MESSAGEPROCESSED (
            serviceName,
            processorName,
            messageId,
            createdAt)
        VALUES (
            '%s',
            ?,
            ?,
            ?)
        """.formatted(serviceName));
    repository.setQueryString("""
        SELECT COUNT(*)
        FROM CAMEL_MESSAGEPROCESSED
        WHERE
            serviceName = '%s' AND
            processorName = ? AND
            messageId = ?
        """.formatted(serviceName));
    repository.setDoneString("""
        UPDATE CAMEL_MESSAGEPROCESSED
        SET done = true, createdAt = CURRENT_TIMESTAMP
        WHERE
            serviceName = '%s' AND
            processorName = ? AND
            messageId = ?
        """.formatted(serviceName));
    return repository;
  }

  @Override
  public void configure() {
    from(file("in").noop(true).idempotent(false))
        .routeId(ROUTE_ID)
        .idempotentConsumer(
            simple("${file:name}"),
            idempotentRepository)
        .setHeader("throw", () -> Boolean.toString(random.nextBoolean()))
        .choice()
            .when(header("throw").isEqualTo("true"))
                .throwException(new RuntimeException())
            .endChoice()
        .end()
        .to(file("out"));
  }
}
