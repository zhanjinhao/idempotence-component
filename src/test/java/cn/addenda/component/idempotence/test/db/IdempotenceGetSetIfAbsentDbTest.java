package cn.addenda.component.idempotence.test.db;

import cn.addenda.component.idempotence.*;
import cn.addenda.component.idempotence.test.IdempotenceTestConfiguration;
import lombok.SneakyThrows;
import org.junit.*;
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
public class IdempotenceGetSetIfAbsentDbTest extends AbstractIdempotenceDbTest {

  static AnnotationConfigApplicationContext context;
  private static final String prefix = "prefix";

  @Configuration
  static class AConfig {
    @Bean
    public DbStateCenter_BEFORE_CONSUMPTION DbStateCenter_BEFORE_CONSUMPTION(DataSource dataSource) {
      return new DbStateCenter_BEFORE_CONSUMPTION(dataSource);
    }

    @Bean
    public DbStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_NULL DbStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_NULL(DataSource dataSource) {
      return new DbStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_NULL(dataSource);
    }

    @Bean
    public DbStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_CONSUMING DbStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_CONSUMING(DataSource dataSource) {
      return new DbStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_CONSUMING(dataSource);
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
  public void test_DbStateCenter_BEFORE_CONSUMPTION() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);

    String rawKey = "DbStateCenter_BEFORE_CONSUMPTION";
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
      Assert.assertEquals(ConsumeStage.BEFORE_CONSUMPTION, idempotenceException.getConsumeStage());
      String expected = "Exception occurred in [BEFORE_CONSUMPTION] stage. GetSetIfAbsent CONSUMING state error.";
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, null);
      assertStateCenterHisEquals(dataSource, rawKey, idempotenceException, null);
      assertExceptionLogEquals(dataSource, rawKey, idempotenceException, expected);
    }

  }


  @Test
  public void test_DbStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_NULL() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "DbStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_NULL";
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
      Assert.assertEquals(ConsumeStage.GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR, idempotenceException.getConsumeStage());
      String expected = "Exception occurred in [GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR] stage. GetSetIfAbsent CONSUMING state error and delete key error.";
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, null);
      assertStateCenterHisEquals(dataSource, rawKey, idempotenceException, "CONSUMING");
      assertExceptionLogEquals(dataSource, rawKey, idempotenceException, expected);
    }
  }


  @Test
  public void test_DbStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_CONSUMING() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "DbStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_CONSUMING";

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
      Assert.assertEquals(ConsumeStage.GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR, idempotenceException.getConsumeStage());
      String expected = "Exception occurred in [GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR] stage. GetSetIfAbsent CONSUMING state error and delete key error.";
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, "CONSUMING");
      assertStateCenterHisEquals(dataSource, rawKey, idempotenceException, null);
      assertExceptionLogEquals(dataSource, rawKey, idempotenceException, expected);
    }
  }

  @After
  public void afterClass() {
    context.close();
  }

}
