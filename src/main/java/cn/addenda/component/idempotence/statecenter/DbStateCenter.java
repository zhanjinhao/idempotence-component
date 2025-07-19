package cn.addenda.component.idempotence.statecenter;

import cn.addenda.component.base.collection.ArrayUtils;
import cn.addenda.component.base.datetime.DateUtils;
import cn.addenda.component.base.exception.SystemException;
import cn.addenda.component.base.jackson.util.JacksonUtils;
import cn.addenda.component.base.util.ConnectionUtils;
import cn.addenda.component.idempotence.*;
import cn.addenda.component.spring.cron.CronBak;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.jdbc.support.SQLExceptionTranslator;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author addenda
 * @since 2023/8/3 14:53
 */
@Slf4j
public class DbStateCenter implements StateCenter, InitializingBean, ApplicationListener<ContextClosedEvent> {

  private final DataSource dataSource;

  private final SQLExceptionTranslator exceptionTranslator;

  private CronBak stateCenterCronBak = null;
  private CronBak stateCenterHisCronBak = null;
  private CronBak exceptionLogCronBak = null;

  @Setter
  private String clearExpiredCron = "0/10 * * * * ?";

  public DbStateCenter(DataSource dataSource) {
    this.dataSource = dataSource;
    this.exceptionTranslator = new SQLErrorCodeSQLExceptionTranslator(this.dataSource);
  }

  private static final String SAVE_SQL =
          "insert into t_idempotence_state_center "
                  + "set `namespace` = ?, `prefix` = ?, `raw_key` = ?, `consume_mode` = ?, `scenario` = ?, `x_id` = ?, `consume_state` = ?, `expire_time` = date_add(now(), interval ? second) ";

  @Override
  @SneakyThrows
  public ConsumeState getSetIfAbsent(IdempotenceParamWrapper param, ConsumeState consumeState) {
    while (true) {
      Connection connection = null;
      boolean originalAutoCommit = false;
      try {
        connection = dataSource.getConnection();
        originalAutoCommit = ConnectionUtils.setAutoCommitFalse(connection);
        try (PreparedStatement ps = connection.prepareStatement(SAVE_SQL)) {
          ps.setString(1, param.getNamespace());
          ps.setString(2, param.getPrefix());
          ps.setString(3, param.getRawKey());
          ps.setString(4, param.getConsumeMode().name());
          ps.setString(5, param.getScenario().name());
          ps.setString(6, param.getXId());
          ps.setString(7, consumeState.name());
          ps.setInt(8, param.getTtlInSecs());
          ps.executeUpdate();
        }
        connection.commit();
        return null;
      } catch (SQLException e) {
        rollback(connection);
        DataAccessException translate = exceptionTranslator.translate(e.getMessage() + "\n", null, e);
        if (!(translate instanceof DuplicateKeyException)) {
          throw e;
        }

        // 没有异常的时候，直接return null了
        // 有异常的时候 且 异常不是DuplicateKeyException，throw出去了
        // 有异常的时候 且 异常是DuplicateKeyException，能执行下面的代码
        StateCenterEntity old = doGet(param, true, connection);
        if (old != null) {
          return old.getConsumeState();
        }
      } finally {
        close(connection, originalAutoCommit);
      }
    }
  }

  private static final String GET_SQL1 =
          "select `id`, `namespace`, `prefix`, `raw_key`, `consume_mode`, `scenario`, `x_id`, `consume_state`, `expire_time`, `create_time`" +
                  " from t_idempotence_state_center "
                  + " where `namespace` = ? and `prefix` = ? and `raw_key` = ?";

  private static final String GET_SQL2 =
          "select `id`, `namespace`, `prefix`, `raw_key`, `consume_mode`, `scenario`, `x_id`, `consume_state`, `expire_time`, `create_time`" +
                  " from t_idempotence_state_center "
                  + " where `namespace` = ? and `prefix` = ? and `raw_key` = ? and `x_id` = ?";

  private StateCenterEntity doGet(IdempotenceParamWrapper param, boolean includeOther, Connection connection)
          throws SQLException {
    String getSql = includeOther ? GET_SQL1 : GET_SQL2;
    try (PreparedStatement preparedStatement = connection.prepareStatement(getSql)) {
      preparedStatement.setString(1, param.getNamespace());
      preparedStatement.setString(2, param.getPrefix());
      preparedStatement.setString(3, param.getRawKey());
      if (!includeOther) {
        preparedStatement.setString(4, param.getXId());
      }
      ResultSet resultSet = preparedStatement.executeQuery();
      Supplier<String> logMsgSupplier = () -> String.format("Get StateCenterEntity from [%s] error. Result has multi records.", JacksonUtils.toStr(param));
      return extractStateCenterEntity(resultSet, logMsgSupplier);
    }
  }

  private StateCenterEntity extractStateCenterEntity(ResultSet resultSet, Supplier<String> logMsgSupplier)
          throws SQLException {
    List<StateCenterEntity> stateCenterEntityList = assembleEntityList(resultSet);
    if (stateCenterEntityList.isEmpty()) {
      return null;
    } else if (stateCenterEntityList.size() > 1) {
      throw SystemException.unExpectedException(logMsgSupplier.get());
    } else {
      return stateCenterEntityList.get(0);
    }
  }

  private List<StateCenterEntity> assembleEntityList(ResultSet resultSet) throws SQLException {
    List<StateCenterEntity> stateCenterEntityList = new ArrayList<>();
    while (resultSet.next()) {
      StateCenterEntity stateCenterEntity = new StateCenterEntity();
      stateCenterEntity.setId(resultSet.getLong("id"));
      stateCenterEntity.setNamespace(resultSet.getString("namespace"));
      stateCenterEntity.setPrefix(resultSet.getString("prefix"));
      stateCenterEntity.setRawKey(resultSet.getString("raw_key"));
      String consumeMode = resultSet.getString("consume_mode");
      stateCenterEntity.setConsumeMode(consumeMode == null ? null : ConsumeMode.valueOf(consumeMode));
      stateCenterEntity.setXId(resultSet.getString("x_id"));
      String scenario = resultSet.getString("scenario");
      stateCenterEntity.setScenario(scenario == null ? null : IdempotenceScenario.valueOf(scenario));
      String consumeState = resultSet.getString("consume_state");
      stateCenterEntity.setConsumeState(consumeState == null ? null : ConsumeState.valueOf(consumeState));
      Timestamp expireTime = resultSet.getTimestamp("expire_time");
      stateCenterEntity.setExpireTime(expireTime == null ? null : DateUtils.timestampToLocalDateTime(expireTime.getTime()));
      Timestamp createTime = resultSet.getTimestamp("create_time");
      stateCenterEntity.setCreateTime(createTime == null ? null : DateUtils.timestampToLocalDateTime(createTime.getTime()));
      stateCenterEntityList.add(stateCenterEntity);
    }

    return stateCenterEntityList;
  }

  /**
   * CAS其他xId的数据，修改expireTime
   */
  private static final String UPDATE_SQL1 =
          "update t_idempotence_state_center "
                  + "set `consume_state` = ?, `expire_time` = date_add(now(), interval ? second), `x_id` = ? " +
                  "where `namespace` = ? and `prefix` = ? and `raw_key` = ? and `consume_state` = ? ";

  /**
   * CAS当前xId的数据，不修改expireTime
   */
  private static final String UPDATE_SQL2 =
          "update t_idempotence_state_center "
                  + "set `consume_state` = ? " +
                  "where `namespace` = ? and `prefix` = ? and `raw_key` = ? and `consume_state` = ? and `x_id` = ? ";

  @Override
  @SneakyThrows
  public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
    return casOther ? doCasState1(param, expected, consumeState) : doCasState2(param, expected, consumeState);
  }

  private boolean doCasState1(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState)
          throws SQLException {
    boolean result;
    Connection connection = null;
    boolean originalAutoCommit = false;
    try {
      connection = dataSource.getConnection();
      originalAutoCommit = ConnectionUtils.setAutoCommitFalse(connection);
      try (PreparedStatement ps = connection.prepareStatement(UPDATE_SQL1)) {
        ps.setString(1, consumeState.name());
        ps.setInt(2, param.getTtlInSecs());
        ps.setString(3, param.getXId());
        ps.setString(4, param.getNamespace());
        ps.setString(5, param.getPrefix());
        ps.setString(6, param.getRawKey());
        ps.setString(7, expected.name());
        result = ps.executeUpdate() == 1;
      }
      connection.commit();
      return result;
    } catch (SQLException e) {
      rollback(connection);
      throw e;
    } finally {
      close(connection, originalAutoCommit);
    }
  }

  private boolean doCasState2(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState)
          throws SQLException {
    boolean result;
    Connection connection = null;
    boolean originalAutoCommit = false;
    try {
      connection = dataSource.getConnection();
      originalAutoCommit = ConnectionUtils.setAutoCommitFalse(connection);
      try (PreparedStatement ps = connection.prepareStatement(UPDATE_SQL2)) {
        ps.setString(1, consumeState.name());
        ps.setString(2, param.getNamespace());
        ps.setString(3, param.getPrefix());
        ps.setString(4, param.getRawKey());
        ps.setString(5, expected.name());
        ps.setString(6, param.getXId());
        result = ps.executeUpdate() == 1;
      }
      connection.commit();
      return result;
    } catch (SQLException e) {
      rollback(connection);
      throw e;
    } finally {
      close(connection, originalAutoCommit);
    }
  }

  @Override
  public void saveExceptionLog(IdempotenceParamWrapper param, Object[] arguments, ConsumeStage consumeStage, String message, Throwable throwable) {
    String argsJson = JacksonUtils.toStr(arguments);
    try {
      doSaveLog(param, consumeStage, argsJson, message, throwable);
    } catch (Exception e) {
      log.error("Save log error [{}]. Scenario: [{}], ConsumeMode: [{}]. ConsumeStage: [{}]. Message: [{}]. Arguments: [{}]. XId: [{}].",
              param.getKey(), param.getScenario(), param.getConsumeMode(), consumeStage, message, argsJson, param.getXId(), e);
    }
  }

  private static final String SAVE_LOG_SQL =
          "insert into t_idempotence_exception_log "
                  + "set `namespace` = ?, `prefix` = ?, `raw_key` = ?, `consume_mode` = ?, `x_id` = ?, `consume_stage` = ?, `scenario` = ?, `args` = ?, `exception_msg` = ?, `exception_stack` = ?, `expire_time` = date_add(now(), interval ? second)";

  private void doSaveLog(IdempotenceParamWrapper param, ConsumeStage consumeStage, String argsJson, String message, Throwable throwable)
          throws SQLException {
    Connection connection = null;
    boolean originalAutoCommit = false;
    try {
      connection = dataSource.getConnection();
      originalAutoCommit = ConnectionUtils.setAutoCommitFalse(connection);
      try (PreparedStatement ps = connection.prepareStatement(SAVE_LOG_SQL)) {
        ps.setString(1, param.getNamespace());
        ps.setString(2, param.getPrefix());
        ps.setString(3, param.getRawKey());
        ps.setString(4, param.getConsumeMode().name());
        ps.setString(5, param.getXId());
        ps.setString(6, consumeStage.name());
        ps.setString(7, param.getScenario().name());
        ps.setObject(8, argsJson);
        if (message.length() > 2048) {
          message = message.substring(0, 2048);
        }
        ps.setString(9, message);
        StringWriter errors = new StringWriter();
        throwable.printStackTrace(new PrintWriter(errors));
        String stack = errors.toString();
        if (stack.length() > 20480) {
          stack = stack.substring(0, 20480);
        }
        ps.setString(10, stack);
        ps.setLong(11, param.getTtlInSecs());
        ps.executeUpdate();
      }
      connection.commit();
    } catch (SQLException e) {
      rollback(connection);
      throw e;
    } finally {
      close(connection, originalAutoCommit);
    }
  }

  private static final String SAVE_HIS_SQL =
          "insert into t_idempotence_state_center_his "
                  + "set `namespace` = ?, `prefix` = ?, `raw_key` = ?, `consume_mode` = ?, `scenario` = ?, `x_id` = ?, `consume_state` = ?, `expire_time` = ?, `create_time` = ? ";

  private static final String DELETE_SQL =
          "delete from t_idempotence_state_center where `id` = ? ";

  @Override
  @SneakyThrows
  public boolean delete(IdempotenceParamWrapper param) {
    Connection connection = null;
    boolean originalAutoCommit = false;
    try {
      connection = dataSource.getConnection();
      originalAutoCommit = ConnectionUtils.setAutoCommitFalse(connection);

      StateCenterEntity stateCenterEntity = doGet(param, false, connection);
      if (stateCenterEntity == null) {
        return false;
      }

      // 删除记录表里的数据
      int result;
      try (PreparedStatement ps = connection.prepareStatement(DELETE_SQL)) {
        ps.setLong(1, stateCenterEntity.getId());
        result = ps.executeUpdate();
      }

      // 数据存到his表里面
      if (result == 1) {
        try (PreparedStatement ps = connection.prepareStatement(SAVE_HIS_SQL)) {
          doSetSaveHistoryPs(stateCenterEntity, ps);
          ps.executeUpdate();
        }
      }

      connection.commit();
    } catch (SQLException e) {
      rollback(connection);
      throw e;
    } finally {
      close(connection, originalAutoCommit);
    }
    return true;
  }

  private void doSetSaveHistoryPs(StateCenterEntity stateCenterEntity, PreparedStatement ps)
          throws SQLException {
    ps.setString(1, stateCenterEntity.getNamespace());
    ps.setString(2, stateCenterEntity.getPrefix());
    ps.setString(3, stateCenterEntity.getRawKey());
    ps.setString(4, stateCenterEntity.getConsumeMode().name());
    ps.setString(5, stateCenterEntity.getScenario().name());
    ps.setString(6, stateCenterEntity.getXId());
    ps.setString(7, stateCenterEntity.getConsumeState().name());
    ps.setObject(8, stateCenterEntity.getExpireTime());
    ps.setObject(9, stateCenterEntity.getCreateTime());
  }

  @Override
  @SneakyThrows
  public void handle(IdempotenceException idempotenceException) {
    IdempotenceKey idempotenceKey = idempotenceException.getIdempotenceKey();
    ConsumeStage consumeStage = idempotenceException.getConsumeStage();
    String xId = idempotenceException.getXId();
    if (consumeStage == ConsumeStage.GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR
            || consumeStage == ConsumeStage.SERVICE_EXCEPTION_AND_DELETE_ERROR) {
      IdempotenceParamWrapper param = new IdempotenceParamWrapper();
      param.setNamespace(idempotenceKey.getNamespace());
      param.setPrefix(idempotenceKey.getPrefix());
      param.setRawKey(idempotenceKey.getRawKey());
      param.setXId(xId);

      delete(param);
    } else if (consumeStage == ConsumeStage.CAS_CONSUMING_TO_EXCEPTION_ERROR
            || consumeStage == ConsumeStage.RETRY_ERROR_AND_RESET_ERROR) {
      IdempotenceParamWrapper param = new IdempotenceParamWrapper();
      param.setNamespace(idempotenceKey.getNamespace());
      param.setPrefix(idempotenceKey.getPrefix());
      param.setRawKey(idempotenceKey.getRawKey());
      param.setXId(xId);

      casState(param, ConsumeState.CONSUMING, ConsumeState.EXCEPTION, false);
    }
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    stateCenterCronBak = new CronBak(clearExpiredCron, dataSource, 200, true,
            "t_idempotence_state_center", "`expire_time` < now()",
            ArrayUtils.asHashSet("id", "namespace", "prefix", "raw_key", "consume_mode", "scenario", "x_id", "consume_state", "expire_time", "create_time"), "id",
            "t_idempotence_state_center_bak");
    stateCenterCronBak.cronClean();
    stateCenterHisCronBak = new CronBak(clearExpiredCron, dataSource, 200, true,
            "t_idempotence_state_center_his", "`expire_time` < now()",
            ArrayUtils.asHashSet("id", "namespace", "prefix", "raw_key", "consume_mode", "scenario", "x_id", "consume_state", "expire_time", "create_time", "delete_time"), "id",
            "t_idempotence_state_center_his_bak");
    stateCenterHisCronBak.cronClean();
    exceptionLogCronBak = new CronBak(clearExpiredCron, dataSource, 200, true,
            "t_idempotence_exception_log", "`expire_time` < now()",
            ArrayUtils.asHashSet("id", "namespace", "prefix", "raw_key", "consume_mode", "x_id", "consume_stage", "scenario", "args", "exception_msg", "exception_stack", "expire_time", "create_time"), "id",
            "t_idempotence_exception_log_bak");
    exceptionLogCronBak.cronClean();
  }

  private void close(Connection connection, boolean originalAutoCommit)
          throws SQLException {
    if (connection == null) {
      return;
    }
    ConnectionUtils.setAutoCommit(connection, originalAutoCommit);
    ConnectionUtils.close(connection);
  }

  private void rollback(Connection connection)
          throws SQLException {
    if (connection != null) {
      connection.rollback();
    }
  }

  @Override
  public void onApplicationEvent(ContextClosedEvent event) {
    stateCenterCronBak.close();
    stateCenterHisCronBak.close();
    exceptionLogCronBak.close();
  }

}
