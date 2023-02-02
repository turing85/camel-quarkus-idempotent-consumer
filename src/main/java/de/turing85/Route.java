package de.turing85;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.file;

import javax.enterprise.context.ApplicationScoped;
import javax.sql.DataSource;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.processor.idempotent.jdbc.JdbcMessageIdRepository;

@ApplicationScoped
public class Route extends RouteBuilder {
  public static final String ROUTE_ID = "file-mover";
  private final JdbcMessageIdRepository idempotentRepository;

  public Route(@SuppressWarnings("CdiInjectionPointsInspection") DataSource dataSource) {
    idempotentRepository =
        constructRepository(dataSource);
  }

  private static JdbcMessageIdRepository constructRepository(DataSource dataSource) {
    final JdbcMessageIdRepository repository =
        new JdbcMessageIdRepository(dataSource, ROUTE_ID);
    repository.setCreateTableIfNotExists(true);
    return repository;
  }

  @Override
  public void configure() {
    from(file("in").noop(true).idempotent(false))
        .routeId(ROUTE_ID)
        .idempotentConsumer(
            header("CamelFileName"),
            idempotentRepository)
        .to(file("out"));
  }
}
