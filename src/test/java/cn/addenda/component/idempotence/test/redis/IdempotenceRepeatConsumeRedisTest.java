package cn.addenda.component.idempotence.test.redis;

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

/**
 * @author addenda
 * @since 2023/9/25 15:33
 */
public class IdempotenceRepeatConsumeRedisTest extends AbstractIdempotenceRedisTest {

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
  public void test_RedisStateCenter_REPEATED_CONSUMPTION() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);
    String rawKey = "RedisStateCenter_REPEATED_CONSUMPTION";

    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.MQ)
              .stateCenter("redisStateCenter")
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {
      });
    } catch (IdempotenceException idempotenceException) {
      throw idempotenceException;
    }
    assertStateCenterEquals(dataSource, rawKey, null, "SUCCESS");

    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.MQ)
              .stateCenter("redisStateCenter")
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {

      });
    } catch (IdempotenceException idempotenceException) {
      Assert.assertEquals(ConsumeStage.REPEATED_CONSUMPTION, idempotenceException.getConsumeStage());
      assertStateCenterEquals(dataSource, rawKey, null, "SUCCESS");
      assertStateCenterEquals(dataSource, rawKey, null, "SUCCESS");
    }

  }


  @Test
  public void test_RedisStateCenter_REPEATED_CONSUMPTION_ServiceException() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);
    String rawKey = "RedisStateCenter_REPEATED_CONSUMPTION_ServiceException";

    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.MQ)
              .stateCenter("redisStateCenter")
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {
      });
    } catch (IdempotenceException idempotenceException) {
      throw idempotenceException;
    }
    assertStateCenterEquals(dataSource, rawKey, null, "SUCCESS");

    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.REQUEST)
              .stateCenter("redisStateCenter")
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {

      });
    } catch (ServiceException e) {
      Assert.assertEquals("数据 [idempotence:prefix:RedisStateCenter_REPEATED_CONSUMPTION_ServiceException] 已处理过！", e.getMessage());
    }

  }

  @After
  public void afterClass() {
    context.close();
  }

}
