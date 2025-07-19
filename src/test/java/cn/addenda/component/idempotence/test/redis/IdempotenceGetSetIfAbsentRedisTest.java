package cn.addenda.component.idempotence.test.redis;

import cn.addenda.component.idempotence.*;
import cn.addenda.component.idempotence.statecenter.RedisStateCenter;
import cn.addenda.component.idempotence.statecenter.StateCenter;
import cn.addenda.component.idempotence.test.IdempotenceTestConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Set;

/**
 * @author addenda
 * @since 2023/9/25 15:33
 */
public class IdempotenceGetSetIfAbsentRedisTest extends AbstractIdempotenceRedisTest {

  static AnnotationConfigApplicationContext context;
  private static final String prefix = "prefix";

  @Configuration
  static class AConfig {
    @Bean
    public RedisStateCenter redisStateCenter(StringRedisTemplate dataSource) {
      return new RedisStateCenter(dataSource);
    }

    @Bean
    public RedisStateCenter_BEFORE_CONSUMPTION RedisStateCenter_BEFORE_CONSUMPTION(StringRedisTemplate dataSource) {
      return new RedisStateCenter_BEFORE_CONSUMPTION(dataSource);
    }

    @Bean
    public RedisStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_NULL RedisStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_NULL(StringRedisTemplate dataSource) {
      return new RedisStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_NULL(dataSource);
    }

    @Bean
    public RedisStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_CONSUMING RedisStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_CONSUMING(StringRedisTemplate dataSource) {
      return new RedisStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_CONSUMING(dataSource);
    }
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
  public void test_RedisStateCenter_BEFORE_CONSUMPTION() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);

    String rawKey = "RedisStateCenter_BEFORE_CONSUMPTION";
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
      assertStateCenterEquals(dataSource, rawKey, null, null);
    }

  }


  @Test
  public void test_RedisStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_NULL() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);
    String rawKey = "RedisStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_NULL";
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
      assertStateCenterEquals(dataSource, rawKey, null, null);

      StateCenter stateCenter = context.getBean("redisStateCenter", StateCenter.class);
      stateCenter.handle(idempotenceException);
      assertStateCenterEquals(dataSource, rawKey, null, null);
    }
  }


  @Test
  public void test_RedisStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_CONSUMING() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);
    String rawKey = "RedisStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_CONSUMING";

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
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, "CONSUMING");

      StateCenter stateCenter = context.getBean("redisStateCenter", StateCenter.class);
      stateCenter.handle(idempotenceException);
      assertStateCenterEquals(dataSource, rawKey, null, null);
    }
  }


  @After
  public void afterClass() {
    context.close();
  }

}
