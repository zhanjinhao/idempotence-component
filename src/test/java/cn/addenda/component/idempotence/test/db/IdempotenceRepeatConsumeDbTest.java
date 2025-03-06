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
public class IdempotenceRepeatConsumeDbTest extends AbstractIdempotenceDbTest {

  static AnnotationConfigApplicationContext context;
  private static final String prefix = "prefix";

  @Configuration
  static class AConfig {
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
  public void test_DbStateCenter_REPEATED_CONSUMPTION() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "DbStateCenter_REPEATED_CONSUMPTION";

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
      Assert.assertEquals(ConsumeStage.REPEATED_CONSUMPTION, idempotenceException.getConsumeStage());
      String expected2 = "Exception occurred in [REPEATED_CONSUMPTION] stage. [idempotence:prefix:DbStateCenter_REPEATED_CONSUMPTION] has consumed.";
      assertStateCenterEquals(dataSource, rawKey, null, "SUCCESS");
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, null);
      assertStateCenterHisEquals(dataSource, rawKey, idempotenceException, null);
      assertStateCenterHisEquals(dataSource, rawKey, null, null);
      assertExceptionLogEquals(dataSource, rawKey, idempotenceException, expected2);
      assertExceptionLogEquals(dataSource, rawKey, null, expected2);
    }

  }


  @Test
  public void test_DbStateCenter_REPEATED_CONSUMPTION_ServiceException() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "DbStateCenter_REPEATED_CONSUMPTION_ServiceException";

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

    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.REQUEST)
              .stateCenter("dbStateCenter")
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {

      });
    } catch (ServiceException e) {
      Assert.assertEquals("数据 [idempotence:prefix:DbStateCenter_REPEATED_CONSUMPTION_ServiceException] 已处理过！", e.getMessage());
    }

  }

  @After
  public void afterClass() {
    context.close();
  }

}
