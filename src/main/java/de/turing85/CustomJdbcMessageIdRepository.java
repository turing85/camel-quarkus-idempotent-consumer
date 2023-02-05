package de.turing85;

import java.sql.Timestamp;
import java.util.Objects;
import javax.sql.DataSource;
import org.apache.camel.CamelContext;
import org.apache.camel.processor.idempotent.jdbc.JdbcOrphanLockAwareIdempotentRepository;
import org.springframework.dao.DataAccessException;

class CustomJdbcMessageIdRepository extends JdbcOrphanLockAwareIdempotentRepository {
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
    setDeleteString("""
        UPDATE CAMEL_MESSAGEPROCESSED
        SET done = 'true', createdAt = CURRENT_TIMESTAMP
        WHERE
            processorName = ? AND
            messageId = ?
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
  protected int insert(String key) {
    return Objects.requireNonNull(transactionTemplate.execute(status -> {
      Object savepoint = status.createSavepoint();
      try {
        return super.insert(key);
      } catch (DataAccessException e) {
        status.rollbackToSavepoint(savepoint);
        return getJdbcTemplate().update(
            getUpdateTimestampQuery(),
            new Timestamp(System.currentTimeMillis()),
            processorName,
            key);
      } finally {
        status.releaseSavepoint(savepoint);
      }
    }));
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
  public boolean confirm(String key) {
    if (super.confirm(key)) {
      // the parent class has a set of message-ids and processor names that is uses to keep entries
      // alive. We can remove an entry by calling the delete-method.
      delete(key);
      return true;
    }
    return false;
  }
}
