package de.turing85;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.file;

import de.turing85.config.ServiceConfig;
import de.turing85.config.IdempotencyConfig;
import javax.enterprise.context.ApplicationScoped;
import javax.sql.DataSource;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class Route extends RouteBuilder {
  public static final String ROUTE_ID = "file-mover";
  private final CustomJdbcMessageIdRepository idempotentRepository;

  public Route(
      @SuppressWarnings("CdiInjectionPointsInspection") DataSource dataSource,
      ServiceConfig serviceConfig,
      CamelContext context) {
    idempotentRepository =
        constructRepository(dataSource, serviceConfig, context);
  }

  private static CustomJdbcMessageIdRepository constructRepository(
      DataSource dataSource,
      ServiceConfig serviceConfig,
      CamelContext context) {
    IdempotencyConfig idempotencyConfig = serviceConfig.idempotencyConfig();
    String serviceName = serviceConfig.name();
    final CustomJdbcMessageIdRepository repository =
        new CustomJdbcMessageIdRepository(dataSource, ROUTE_ID, context);
    repository.setTableName(idempotencyConfig.tableName());
    repository.setLockMaxAgeMillis(idempotencyConfig.lockMaxAge().toMillis());
    repository.setLockKeepAliveIntervalMillis(idempotencyConfig.keepAliveInterval().toMillis());
    repository.setCreateTableIfNotExists(false);
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
    repository.setDeleteString("""
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
        .to(file("out"));
  }
}
