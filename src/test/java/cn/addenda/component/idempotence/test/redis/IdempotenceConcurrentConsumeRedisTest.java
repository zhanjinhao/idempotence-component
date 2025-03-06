package cn.addenda.component.idempotence.test.redis;

import cn.addenda.component.base.concurrent.SleepUtils;
import cn.addenda.component.base.exception.ServiceException;
import cn.addenda.component.idempotence.*;
import cn.addenda.component.idempotence.test.IdempotenceTestConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author addenda
 * @since 2023/9/25 15:33
 */
public class IdempotenceConcurrentConsumeRedisTest extends AbstractIdempotenceRedisTest {

  static AnnotationConfigApplicationContext context;
  private static final String prefix = "prefix";

  @Configuration
  static class AConfig {

  }

  @Before
  public void beforeClass() {
    context = new AnnotationConfigApplicationContext();
    context.register(IdempotenceTestConfiguration.class);
    context.register(AConfig.class);
    context.refresh();

    StringRedisTemplate stringRedisTemplate = context.getBean(StringRedisTemplate.class);
    Set<String> keys = stringRedisTemplate.keys("idempotence" + ":" + prefix + "*");
    if (keys != null) {
      for (String key : keys) {
        stringRedisTemplate.delete(key);
      }
    }
  }


  @Test
  public void test_RedisStateCenter_WAITING_TIMEOUT() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);
    String rawKey = "RedisStateCenter_WAITING_TIMEOUT";

    Thread thread = new Thread(() -> {
      try {
        IdempotenceAttr build = IdempotenceAttr.builder()
                .prefix(prefix)
                .spEL(rawKey)
                .scenario(IdempotenceScenario.MQ)
                .stateCenter("redisStateCenter")
                .consumeMode(ConsumeMode.SUCCESS)
                .build();
        idempotenceHelper.idempotent(build, () -> {
          SleepUtils.sleep(TimeUnit.MILLISECONDS, 10000);
        });
      } catch (IdempotenceException idempotenceException) {
        throw idempotenceException;
      }
      assertStateCenterEquals(dataSource, rawKey, null, "SUCCESS");
    });
    thread.start();
    SleepUtils.sleep(TimeUnit.MILLISECONDS, 2000);


    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.MQ)
              .stateCenter("redisStateCenter")
              .consumeMode(ConsumeMode.SUCCESS)
              .expectCost(5)
              .timeUnit(TimeUnit.SECONDS)
              .build();
      idempotenceHelper.idempotent(build, () -> {

      });
    } catch (IdempotenceException idempotenceException) {
      Assert.assertEquals(ConsumeStage.WAIT_TIMEOUT, idempotenceException.getConsumeStage());
      assertStateCenterEquals(dataSource, rawKey, null, "CONSUMING");
      assertStateCenterEquals(dataSource, rawKey, null, "CONSUMING");
    }

    thread.join();

  }


  @Test
  public void test_RedisStateCenter_CONCURRENT_CONSUMPTION_ServiceException() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);
    String rawKey = "RedisStateCenter_CONCURRENT_CONSUMPTION_ServiceException";

    Thread thread = new Thread(() -> {
      try {
        IdempotenceAttr build = IdempotenceAttr.builder()
                .prefix(prefix)
                .spEL(rawKey)
                .scenario(IdempotenceScenario.MQ)
                .stateCenter("redisStateCenter")
                .consumeMode(ConsumeMode.SUCCESS)
                .build();
        idempotenceHelper.idempotent(build, () -> {
          SleepUtils.sleep(TimeUnit.MILLISECONDS, 10000);
        });
      } catch (IdempotenceException idempotenceException) {
        throw idempotenceException;
      }
      assertStateCenterEquals(dataSource, rawKey, null, "SUCCESS");
    });
    thread.start();
    SleepUtils.sleep(TimeUnit.MILLISECONDS, 2000);


    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.REQUEST)
              .stateCenter("redisStateCenter")
              .consumeMode(ConsumeMode.SUCCESS)
              .expectCost(5)
              .timeUnit(TimeUnit.SECONDS)
              .build();
      idempotenceHelper.idempotent(build, () -> {

      });
    } catch (ServiceException serviceException) {
      Assert.assertEquals("数据 [idempotence:prefix:RedisStateCenter_CONCURRENT_CONSUMPTION_ServiceException] 正在消费，请稍后再试！", serviceException.getMessage());
      assertStateCenterEquals(dataSource, rawKey, null, "CONSUMING");
    }

    thread.join();

  }

  @After
  public void afterClass() {
    context.close();
  }

}
