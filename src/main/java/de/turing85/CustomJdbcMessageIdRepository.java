package de.turing85;

import java.sql.Timestamp;
import java.util.Objects;
import javax.sql.DataSource;
import lombok.Getter;
import lombok.Setter;
import org.apache.camel.CamelContext;
import org.apache.camel.processor.idempotent.jdbc.JdbcOrphanLockAwareIdempotentRepository;

@Getter
@Setter
class CustomJdbcMessageIdRepository extends JdbcOrphanLockAwareIdempotentRepository {
  private String doneString;

  public CustomJdbcMessageIdRepository(
      DataSource dataSource,
      String processorName,
      CamelContext camelContext) {
    super(dataSource, processorName, camelContext);
    initializeQueries();
  }

  private void initializeQueries() {
    setCreateString("""
        CREATE TABLE CAMEL_MESSAGEPROCESSED (
            processorName VARCHAR(255),
            messageId VARCHAR(100),
            createdAt TIMESTAMP,
            done BOOLEAN DEFAULT false,
            PRIMARY KEY (processorName, messageId)
        )
        """);
    setInsertString("""
        INSERT INTO CAMEL_MESSAGEPROCESSED (
            processorName,
            messageId,
            createdAt)
        VALUES (?, ?, ?)
        """);
    setDoneString("""
        UPDATE CAMEL_MESSAGEPROCESSED
        SET done = 'true', createdAt = CURRENT_TIMESTAMP
        WHERE
            processorName = ? AND
            messageId = ?
        """);

    // This is a query that will never return anything, thus nothing is deleted.
    // We finalize entries through the updateString query instead, which we call explicitly.
    setDeleteString("""
        DELETE FROM CAMEL_MESSAGEPROCESSED
        WHERE
            ? = ?
            AND 1 != 1
        """);
    setQueryString("""
        SELECT COUNT(*)
        FROM CAMEL_MESSAGEPROCESSED
        WHERE
            processorName = ? AND
            messageId = ?
        """);
  }

  @Override
  protected int queryForInt(String key) {
    // If the entry is {@code done}, the lock is never orphaned.
    // If the update timestamp time is more than lockMaxAge then assume that the lock is orphan and
    // the process which had acquired the lock has died.
    String orphanLockRecoverQueryString =
        getQueryString() + " AND (done = 'true' OR createdAt >= ?)";
    Timestamp xMillisAgo = new Timestamp(System.currentTimeMillis() - getLockMaxAgeMillis());
    return Objects.requireNonNull(jdbcTemplate.queryForObject(
        orphanLockRecoverQueryString,
        Integer.class,
        processorName,
        key,
        xMillisAgo));
  }

  @Override
  public void doInit() throws Exception {
    super.doInit();
    if (this.getTableName() != null) {
      this.doneString = this.doneString.replaceFirst("CAMEL_MESSAGEPROCESSED", this.getTableName());
    }

  }

  @Override
  public boolean confirm(String key) {
    if (super.confirm(key)) {
      jdbcTemplate.update(getDoneString(), getProcessorName(), key);
      // the parent class has a set of message-ids and processor names that is uses to keep entries
      // alive. We can remove an entry by calling the delete-method. But this will - in return -
      // execute the deleteString query. This is the reason we constucts a deleteString query that
      // never anything.
      delete(key);
      return true;
    }
    return false;
  }
}
