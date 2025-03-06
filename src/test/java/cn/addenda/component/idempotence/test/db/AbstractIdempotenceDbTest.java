package cn.addenda.component.idempotence.test.db;

import cn.addenda.component.idempotence.ConsumeState;
import cn.addenda.component.idempotence.IdempotenceException;
import cn.addenda.component.idempotence.IdempotenceParamWrapper;
import cn.addenda.component.idempotence.statecenter.DbStateCenter;
import lombok.SneakyThrows;
import org.junit.Assert;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public abstract class AbstractIdempotenceDbTest {

  static class DbStateCenter_BEFORE_CONSUMPTION extends DbStateCenter {
    public DbStateCenter_BEFORE_CONSUMPTION(DataSource dataSource) {
      super(dataSource);
    }

    @Override
    public ConsumeState getSetIfAbsent(IdempotenceParamWrapper param, ConsumeState consumeState) {
      throw new RuntimeException("getSetIfAbsent");
    }
  }

  static class DbStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_NULL extends DbStateCenter {
    public DbStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_NULL(DataSource dataSource) {
      super(dataSource);
    }

    @Override
    public ConsumeState getSetIfAbsent(IdempotenceParamWrapper param, ConsumeState consumeState) {
      super.getSetIfAbsent(param, consumeState);
      throw new RuntimeException("getSetIfAbsent");
    }


    @Override
    public boolean delete(IdempotenceParamWrapper param) {
      super.delete(param);
      throw new RuntimeException("delete");
    }
  }

  static class DbStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_CONSUMING extends DbStateCenter {
    public DbStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_CONSUMING(DataSource dataSource) {
      super(dataSource);
    }

    @Override
    public ConsumeState getSetIfAbsent(IdempotenceParamWrapper param, ConsumeState consumeState) {
      super.getSetIfAbsent(param, consumeState);
      throw new RuntimeException("getSetIfAbsent");
    }

    @Override
    public boolean delete(IdempotenceParamWrapper param) {
      throw new RuntimeException("delete");
    }
  }

  static class DbStateCenter_SERVICE_EXCEPTION_AND_DELETE_ERROR extends DbStateCenter {
    public DbStateCenter_SERVICE_EXCEPTION_AND_DELETE_ERROR(DataSource dataSource) {
      super(dataSource);
    }

    @Override
    public boolean delete(IdempotenceParamWrapper param) {
      throw new RuntimeException("delete");
    }
  }

  static class DbStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_CONSUMING extends DbStateCenter {
    public DbStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_CONSUMING(DataSource dataSource) {
      super(dataSource);
    }

    @Override
    public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
      if (expected == ConsumeState.CONSUMING && consumeState == ConsumeState.EXCEPTION) {
        throw new RuntimeException("for purpose! ");
      }
      return super.casState(param, expected, consumeState, casOther);
    }
  }

  static class DbStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_EXCEPTION extends DbStateCenter {
    public DbStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_EXCEPTION(DataSource dataSource) {
      super(dataSource);
    }

    @Override
    public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
      boolean b = super.casState(param, expected, consumeState, casOther);
      if (expected == ConsumeState.CONSUMING && consumeState == ConsumeState.EXCEPTION) {
        throw new RuntimeException("for purpose! ");
      }
      return b;
    }
  }

  static class DbStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_SUCCESS extends DbStateCenter {
    public DbStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_SUCCESS(DataSource dataSource) {
      super(dataSource);
    }

    @Override
    public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
      boolean b = super.casState(param, expected, consumeState, casOther);
      if (expected == ConsumeState.CONSUMING && consumeState == ConsumeState.SUCCESS) {
        throw new RuntimeException("for purpose! ");
      }
      return b;
    }
  }

  static class DbStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_CONSUMING extends DbStateCenter {
    public DbStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_CONSUMING(DataSource dataSource) {
      super(dataSource);
    }

    @Override
    public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
      if (expected == ConsumeState.CONSUMING && consumeState == ConsumeState.SUCCESS) {
        throw new RuntimeException("for purpose! ");
      }
      return super.casState(param, expected, consumeState, casOther);
    }
  }

  static class DbStateCenter_RETRY_ERROR extends DbStateCenter {
    public DbStateCenter_RETRY_ERROR(DataSource dataSource) {
      super(dataSource);
    }

    @Override
    public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
      boolean b = super.casState(param, expected, consumeState, casOther);
      if (expected == ConsumeState.EXCEPTION && consumeState == ConsumeState.CONSUMING) {
        throw new RuntimeException("for purpose! ");
      }
      return b;
    }
  }

  static class DbStateCenter_RETRY_ERROR_AND_RESET_ERROR_EXCEPTION extends DbStateCenter {
    public DbStateCenter_RETRY_ERROR_AND_RESET_ERROR_EXCEPTION(DataSource dataSource) {
      super(dataSource);
    }

    @Override
    public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
      boolean b = super.casState(param, expected, consumeState, casOther);
      if (expected == ConsumeState.EXCEPTION && consumeState == ConsumeState.CONSUMING) {
        throw new RuntimeException("for purpose! ");
      }
      if (expected == ConsumeState.CONSUMING && consumeState == ConsumeState.EXCEPTION) {
        throw new RuntimeException("for purpose! ");
      }
      return b;
    }
  }

  static class DbStateCenter_RETRY_ERROR_AND_RESET_ERROR_CONSUMING extends DbStateCenter {
    public DbStateCenter_RETRY_ERROR_AND_RESET_ERROR_CONSUMING(DataSource dataSource) {
      super(dataSource);
    }

    @Override
    public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
      if (expected == ConsumeState.CONSUMING && consumeState == ConsumeState.EXCEPTION) {
        throw new RuntimeException("for purpose! ");
      }
      boolean b = super.casState(param, expected, consumeState, casOther);
      if (expected == ConsumeState.EXCEPTION && consumeState == ConsumeState.CONSUMING) {
        throw new RuntimeException("for purpose! ");
      }
      return b;
    }
  }


  @SneakyThrows
  protected void assertStateCenterEquals(DataSource dataSource, String rawKey, IdempotenceException idempotenceException,
                                         String expected) {
    try (Connection connection = dataSource.getConnection()) {
      String sql1 = "select * from t_idempotence_state_center " +
              "where namespace = 'idempotence' and prefix = 'prefix' and raw_key = ?";
      String sql2 = "select * from t_idempotence_state_center " +
              "where namespace = 'idempotence' and prefix = 'prefix' and raw_key = ? and x_id = ?";
      PreparedStatement ps = connection.prepareStatement(idempotenceException != null ? sql2 : sql1);

      ps.setString(1, rawKey);
      if (idempotenceException != null) {
        ps.setString(2, idempotenceException.getXId());
      }
      ResultSet resultSet = ps.executeQuery();

      String a = null;
      while (resultSet.next()) {
        a = resultSet.getString("consume_state");
      }
      Assert.assertEquals(expected, a);
    }
  }

  @SneakyThrows
  protected void assertStateCenterHisEquals(DataSource dataSource, String rawKey, IdempotenceException idempotenceException,
                                            String expected) {
    try (Connection connection = dataSource.getConnection()) {
      String sql1 = "select * from t_idempotence_state_center_his " +
              "where namespace = 'idempotence' and prefix = 'prefix' and raw_key = ?";
      String sql2 = "select * from t_idempotence_state_center_his " +
              "where namespace = 'idempotence' and prefix = 'prefix' and raw_key = ? and x_id = ?";
      PreparedStatement ps = connection.prepareStatement(idempotenceException != null ? sql2 : sql1);

      ps.setString(1, rawKey);
      if (idempotenceException != null) {
        ps.setString(2, idempotenceException.getXId());
      }
      ResultSet resultSet = ps.executeQuery();

      String a = null;
      while (resultSet.next()) {
        a = resultSet.getString("consume_state");
      }
      Assert.assertEquals(expected, a);
    }
  }

  @SneakyThrows
  protected void assertExceptionLogEquals(DataSource dataSource, String rawKey, IdempotenceException idempotenceException,
                                          String expected) {
    try (Connection connection = dataSource.getConnection()) {
      String sql1 = "select * from t_idempotence_exception_log " +
              "where namespace = 'idempotence' and prefix = 'prefix' and raw_key = ?";
      String sql2 = "select * from t_idempotence_exception_log " +
              "where namespace = 'idempotence' and prefix = 'prefix' and raw_key = ? and x_id = ?";
      PreparedStatement ps = connection.prepareStatement(idempotenceException != null ? sql2 : sql1);

      ps.setString(1, rawKey);
      if (idempotenceException != null) {
        ps.setString(2, idempotenceException.getXId());
      }
      ResultSet resultSet = ps.executeQuery();

      String a = null;
      while (resultSet.next()) {
        a = resultSet.getString("exception_msg");
      }
      Assert.assertEquals(expected, a);
    }

  }

}
