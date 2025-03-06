package cn.addenda.component.idempotence.test.db;

import cn.addenda.component.base.exception.ServiceException;
import cn.addenda.component.idempotence.*;
import cn.addenda.component.idempotence.test.IdempotenceTestConfiguration;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

/**
 * @author addenda
 * @since 2023/9/25 15:33
 */
public class IdempotenceConsumeDbTest extends AbstractIdempotenceDbTest {

  static AnnotationConfigApplicationContext context;
  private static final String prefix = "prefix";

  @Configuration
  static class AConfig {
    @Bean
    public DbStateCenter_SERVICE_EXCEPTION_AND_DELETE_ERROR DbStateCenter_SERVICE_EXCEPTION_AND_DELETE_ERROR(DataSource dataSource) {
      return new DbStateCenter_SERVICE_EXCEPTION_AND_DELETE_ERROR(dataSource);
    }

    @Bean
    public DbStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_CONSUMING DbStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_CONSUMING(DataSource dataSource) {
      return new DbStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_CONSUMING(dataSource);
    }

    @Bean
    public DbStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_EXCEPTION DbStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_EXCEPTION(DataSource dataSource) {
      return new DbStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_EXCEPTION(dataSource);
    }

    @Bean
    public DbStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_SUCCESS DbStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_SUCCESS(DataSource dataSource) {
      return new DbStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_SUCCESS(dataSource);
    }

    @Bean
    public DbStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_CONSUMING DbStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_CONSUMING(DataSource dataSource) {
      return new DbStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_CONSUMING(dataSource);
    }

  }

  @Before
  @SneakyThrows
  public void beforeClass() {
    context = new AnnotationConfigApplicationContext();
    context.register(IdempotenceTestConfiguration.class);
    context.register(AConfig.class);
    context.refresh();

    DataSource dataSource = context.getBean(DataSource.class);
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      Statement statement = connection.createStatement();
      statement.execute("delete from t_idempotence_state_center");
      statement.execute("delete from t_idempotence_state_center_his");
      statement.execute("delete from t_idempotence_exception_log");
      connection.commit();
    }
  }


  @Test
  public void test_ServiceException() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);

    String rawKey = "ServiceException";
    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.REQUEST)
              .stateCenter("dbStateCenter")
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {
        throw new ServiceException("for purpose! ");
      });
    } catch (ServiceException serviceException) {
      Assert.assertEquals(serviceException.getClass(), ServiceException.class);
    }

    assertStateCenterEquals(dataSource, rawKey, null, null);
    assertStateCenterHisEquals(dataSource, rawKey, null, "CONSUMING");
    assertExceptionLogEquals(dataSource, rawKey, null, null);
  }


  @Test
  public void test_SERVICE_EXCEPTION_AND_DELETE_ERROR() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);

    String rawKey = "DbStateCenter_SERVICE_EXCEPTION_AND_DELETE_ERROR";
    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.REQUEST)
              .stateCenter(rawKey)
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {
        throw new ServiceException("for purpose! ");
      });
    } catch (IdempotenceException idempotenceException) {
      assertStateCenterHisEquals(dataSource, rawKey, idempotenceException, null);
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, "CONSUMING");
      assertExceptionLogEquals(dataSource, rawKey, idempotenceException, "Exception occurred in [SERVICE_EXCEPTION_AND_DELETE_ERROR] stage. ServiceException and delete error.");
    }
  }


  @Test
  public void test_BIZCONSUME_THROWABLE() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "BIZCONSUME_THROWABLE";
    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.MQ)
              .stateCenter("dbStateCenter")
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {
        throw new IllegalArgumentException("for purpose!");
      });
    } catch (IllegalArgumentException illegalArgumentException) {
      Assert.assertEquals(IllegalArgumentException.class, illegalArgumentException.getClass());
    }

    String expected = "Exception occurred in [IN_CONSUMPTION] stage. Biz consumption error.";
    assertStateCenterEquals(dataSource, rawKey, null, "EXCEPTION");
    assertStateCenterHisEquals(dataSource, rawKey, null, null);
    assertExceptionLogEquals(dataSource, rawKey, null, expected);
  }


  @Test
  public void test_DbStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_CONSUMING() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "DbStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_CONSUMING";

    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.MQ)
              .stateCenter(rawKey)
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {
        throw new IllegalArgumentException("for purpose!");
      });
    } catch (IdempotenceException idempotenceException) {
      Assert.assertEquals(ConsumeStage.CAS_CONSUMING_TO_EXCEPTION_ERROR, idempotenceException.getConsumeStage());
      String expected = "Exception occurred in [CAS_CONSUMING_TO_EXCEPTION_ERROR] stage. CAS CONSUMING to EXCEPTION error.";
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, "CONSUMING");
      assertStateCenterHisEquals(dataSource, rawKey, idempotenceException, null);
      assertExceptionLogEquals(dataSource, rawKey, idempotenceException, expected);
    }
  }


  @Test
  public void test_DbStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_EXCEPTION() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "DbStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_EXCEPTION";

    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.MQ)
              .stateCenter(rawKey)
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {
        throw new IllegalArgumentException("for purpose!");
      });
    } catch (IdempotenceException idempotenceException) {
      Assert.assertEquals(ConsumeStage.CAS_CONSUMING_TO_EXCEPTION_ERROR, idempotenceException.getConsumeStage());
      String expected = "Exception occurred in [CAS_CONSUMING_TO_EXCEPTION_ERROR] stage. CAS CONSUMING to EXCEPTION error.";
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, "EXCEPTION");
      assertStateCenterHisEquals(dataSource, rawKey, idempotenceException, null);
      assertExceptionLogEquals(dataSource, rawKey, idempotenceException, expected);
    }
  }


  @Test
  public void test_DbStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_CONSUMING() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "DbStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_CONSUMING";

    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.MQ)
              .stateCenter(rawKey)
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {

      });
    } catch (IdempotenceException idempotenceException) {
      throw idempotenceException;
    }


    String expected = "Exception occurred in [AFTER_CONSUMPTION] stage. CAS CONSUMING to SUCCESS error.";
    assertStateCenterEquals(dataSource, rawKey, null, "CONSUMING");
    assertStateCenterHisEquals(dataSource, rawKey, null, null);
    assertExceptionLogEquals(dataSource, rawKey, null, expected);
  }


  @Test
  public void test_DbStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_SUCCESS() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "DbStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_SUCCESS";

    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.MQ)
              .stateCenter(rawKey)
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {
      });
    } catch (IdempotenceException idempotenceException) {
      throw idempotenceException;
    }
    String expected = "Exception occurred in [AFTER_CONSUMPTION] stage. CAS CONSUMING to SUCCESS error.";
    assertStateCenterEquals(dataSource, rawKey, null, "SUCCESS");
    assertStateCenterHisEquals(dataSource, rawKey, null, null);
    assertExceptionLogEquals(dataSource, rawKey, null, expected);
  }


  @Test
  public void test_DbStateCenter_CAS_CONSUMING_TO_SUCCESS_SUCCESS() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "DbStateCenter_CAS_CONSUMING_TO_SUCCESS_SUCCESS";

    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.MQ)
              .stateCenter("dbStateCenter")
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {
      });
    } catch (IdempotenceException idempotenceException) {
      throw idempotenceException;
    }
    assertStateCenterEquals(dataSource, rawKey, null, "SUCCESS");
    assertStateCenterHisEquals(dataSource, rawKey, null, null);
    assertExceptionLogEquals(dataSource, rawKey, null, null);
  }


  @After
  public void afterClass() {
    context.close();
  }

}
