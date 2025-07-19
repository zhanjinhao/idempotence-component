package cn.addenda.component.idempotence.test.db;

import cn.addenda.component.idempotence.*;
import cn.addenda.component.idempotence.statecenter.DbStateCenter;
import cn.addenda.component.idempotence.statecenter.StateCenter;
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
public class IdempotenceReConsumeAfterExceptionDbTest extends AbstractIdempotenceDbTest {

  static AnnotationConfigApplicationContext context;
  private static final String prefix = "prefix";

  @Configuration
  static class AConfig {
    @Bean
    public DbStateCenter dbStateCenter(DataSource dataSource) {
      return new DbStateCenter(dataSource);
    }

    @Bean
    public DbStateCenter_RETRY_ERROR DbStateCenter_RETRY_ERROR(DataSource dataSource) {
      return new DbStateCenter_RETRY_ERROR(dataSource);
    }

    @Bean
    public DbStateCenter_RETRY_ERROR_AND_RESET_ERROR_EXCEPTION DbStateCenter_RETRY_ERROR_AND_RESET_ERROR_EXCEPTION(DataSource dataSource) {
      return new DbStateCenter_RETRY_ERROR_AND_RESET_ERROR_EXCEPTION(dataSource);
    }

    @Bean
    public DbStateCenter_RETRY_ERROR_AND_RESET_ERROR_CONSUMING DbStateCenter_RETRY_ERROR_AND_RESET_ERROR_CONSUMING(DataSource dataSource) {
      return new DbStateCenter_RETRY_ERROR_AND_RESET_ERROR_CONSUMING(dataSource);
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
  public void test_DbStateCenter_RETRY_ERROR() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "DbStateCenter_RETRY_ERROR";
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
    String expected1 = "Exception occurred in [IN_CONSUMPTION] stage. Biz consumption error.";
    assertStateCenterEquals(dataSource, rawKey, null, "EXCEPTION");
    assertStateCenterHisEquals(dataSource, rawKey, null, null);
    assertExceptionLogEquals(dataSource, rawKey, null, expected1);

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
      Assert.assertEquals(ConsumeStage.RETRY_ERROR, idempotenceException.getConsumeStage());
      String expected2 = "Exception occurred in [RETRY_ERROR] stage. CAS EXCEPTION to CONSUMING error.";
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, "EXCEPTION");
      assertStateCenterHisEquals(dataSource, rawKey, idempotenceException, null);
      assertExceptionLogEquals(dataSource, rawKey, idempotenceException, expected2);
    }

  }


  @Test
  public void test_DbStateCenter_RETRY_ERROR_AND_RESET_ERROR_EXCEPTION() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "DbStateCenter_RETRY_ERROR_AND_RESET_ERROR_EXCEPTION";
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
    String expected1 = "Exception occurred in [IN_CONSUMPTION] stage. Biz consumption error.";
    assertStateCenterEquals(dataSource, rawKey, null, "EXCEPTION");
    assertStateCenterHisEquals(dataSource, rawKey, null, null);
    assertExceptionLogEquals(dataSource, rawKey, null, expected1);

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
      Assert.assertEquals(ConsumeStage.RETRY_ERROR_AND_RESET_ERROR, idempotenceException.getConsumeStage());
      String expected2 = "Exception occurred in [RETRY_ERROR_AND_RESET_ERROR] stage. Reset CONSUMING to EXCEPTION error.";
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, "EXCEPTION");
      assertStateCenterHisEquals(dataSource, rawKey, idempotenceException, null);
      assertExceptionLogEquals(dataSource, rawKey, idempotenceException, expected2);

      StateCenter stateCenter = context.getBean("dbStateCenter", StateCenter.class);
      stateCenter.handle(idempotenceException);
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, "EXCEPTION");
      assertStateCenterHisEquals(dataSource, rawKey, idempotenceException, null);
    }

  }


  @Test
  public void test_DbStateCenter_RETRY_ERROR_AND_RESET_ERROR_CONSUMING() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "DbStateCenter_RETRY_ERROR_AND_RESET_ERROR_CONSUMING";
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
    String expected1 = "Exception occurred in [IN_CONSUMPTION] stage. Biz consumption error.";
    assertStateCenterEquals(dataSource, rawKey, null, "EXCEPTION");
    assertStateCenterHisEquals(dataSource, rawKey, null, null);
    assertExceptionLogEquals(dataSource, rawKey, null, expected1);

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
      Assert.assertEquals(ConsumeStage.RETRY_ERROR_AND_RESET_ERROR, idempotenceException.getConsumeStage());
      String expected2 = "Exception occurred in [RETRY_ERROR_AND_RESET_ERROR] stage. Reset CONSUMING to EXCEPTION error.";
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, "CONSUMING");
      assertStateCenterHisEquals(dataSource, rawKey, idempotenceException, null);
      assertExceptionLogEquals(dataSource, rawKey, idempotenceException, expected2);

      StateCenter stateCenter = context.getBean("dbStateCenter", StateCenter.class);
      stateCenter.handle(idempotenceException);
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, "EXCEPTION");
      assertStateCenterHisEquals(dataSource, rawKey, idempotenceException, null);
    }

  }

  @After
  public void afterClass() {
    context.close();
  }

}
