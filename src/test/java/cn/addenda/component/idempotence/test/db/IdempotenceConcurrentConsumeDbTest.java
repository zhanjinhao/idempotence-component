package cn.addenda.component.idempotence.test.db;

import cn.addenda.component.base.concurrent.SleepUtils;
import cn.addenda.component.base.exception.ServiceException;
import cn.addenda.component.idempotence.*;
import cn.addenda.component.idempotence.test.IdempotenceTestConfiguration;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * @author addenda
 * @since 2023/9/25 15:33
 */
public class IdempotenceConcurrentConsumeDbTest extends AbstractIdempotenceDbTest {

  static AnnotationConfigApplicationContext context;
  private static final String prefix = "prefix";

  @Configuration
  static class AConfig {

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
  public void test_DbStateCenter_WAITING_TIMEOUT() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "DbStateCenter_WAITING_TIMEOUT";

    Thread thread = new Thread(() -> {
      try {
        IdempotenceAttr build = IdempotenceAttr.builder()
                .prefix(prefix)
                .spEL(rawKey)
                .scenario(IdempotenceScenario.MQ)
                .stateCenter("dbStateCenter")
                .consumeMode(ConsumeMode.SUCCESS)
                .build();
        idempotenceHelper.idempotent(build, () -> {
          SleepUtils.sleep(TimeUnit.MILLISECONDS, 10000);
        });
      } catch (IdempotenceException idempotenceException) {
        throw idempotenceException;
      }
      String expected2 = "Exception occurred in [WAIT_TIMEOUT] stage. [idempotence:prefix:DbStateCenter_WAITING_TIMEOUT] has always been in consumption. Expected cost: [5000 ms].";
      assertStateCenterEquals(dataSource, rawKey, null, "SUCCESS");
      assertStateCenterHisEquals(dataSource, rawKey, null, null);
      assertExceptionLogEquals(dataSource, rawKey, null, expected2);
    });
    thread.start();
    SleepUtils.sleep(TimeUnit.MILLISECONDS, 2000);


    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.MQ)
              .stateCenter("dbStateCenter")
              .consumeMode(ConsumeMode.SUCCESS)
              .expectCost(5)
              .timeUnit(TimeUnit.SECONDS)
              .build();
      idempotenceHelper.idempotent(build, () -> {

      });
    } catch (IdempotenceException idempotenceException) {
      Assert.assertEquals(ConsumeStage.WAIT_TIMEOUT, idempotenceException.getConsumeStage());
      String expected2 = "Exception occurred in [WAIT_TIMEOUT] stage. [idempotence:prefix:DbStateCenter_WAITING_TIMEOUT] has always been in consumption. Expected cost: [5000 ms].";
      assertStateCenterEquals(dataSource, rawKey, null, "CONSUMING");
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, null);
      assertStateCenterHisEquals(dataSource, rawKey, null, null);
      assertStateCenterHisEquals(dataSource, rawKey, idempotenceException, null);
      assertExceptionLogEquals(dataSource, rawKey, idempotenceException, expected2);
    }

    thread.join();

  }


  @Test
  public void test_DbStateCenter_CONCURRENT_CONSUMPTION_ServiceException() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    DataSource dataSource = context.getBean(DataSource.class);
    String rawKey = "DbStateCenter_CONCURRENT_CONSUMPTION_ServiceException";

    Thread thread = new Thread(() -> {
      try {
        IdempotenceAttr build = IdempotenceAttr.builder()
                .prefix(prefix)
                .spEL(rawKey)
                .scenario(IdempotenceScenario.MQ)
                .stateCenter("dbStateCenter")
                .consumeMode(ConsumeMode.SUCCESS)
                .build();
        idempotenceHelper.idempotent(build, () -> {
          SleepUtils.sleep(TimeUnit.MILLISECONDS, 10000);
        });
      } catch (IdempotenceException idempotenceException) {
        throw idempotenceException;
      }
      assertStateCenterEquals(dataSource, rawKey, null, "SUCCESS");
      assertStateCenterHisEquals(dataSource, rawKey, null, null);
      assertExceptionLogEquals(dataSource, rawKey, null, null);
    });
    thread.start();
    SleepUtils.sleep(TimeUnit.MILLISECONDS, 2000);


    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.REQUEST)
              .stateCenter("dbStateCenter")
              .consumeMode(ConsumeMode.SUCCESS)
              .expectCost(5)
              .timeUnit(TimeUnit.SECONDS)
              .build();
      idempotenceHelper.idempotent(build, () -> {

      });
    } catch (ServiceException serviceException) {
      Assert.assertEquals("数据 [idempotence:prefix:DbStateCenter_CONCURRENT_CONSUMPTION_ServiceException] 正在消费，请稍后再试！", serviceException.getMessage());
      assertStateCenterEquals(dataSource, rawKey, null, "CONSUMING");
      assertStateCenterHisEquals(dataSource, rawKey, null, null);
      assertExceptionLogEquals(dataSource, rawKey, null, null);
    }

    thread.join();

  }

  @After
  public void afterClass() {
    context.close();
  }

}
