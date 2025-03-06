package cn.addenda.component.idempotence.test.redis;

import cn.addenda.component.base.exception.ServiceException;
import cn.addenda.component.idempotence.*;
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
public class IdempotenceConsumeRedisTest extends AbstractIdempotenceRedisTest {

  static AnnotationConfigApplicationContext context;
  private static final String prefix = "prefix";

  @Configuration
  static class AConfig {
    @Bean
    public RedisStateCenter_SERVICE_EXCEPTION_AND_DELETE_ERROR RedisStateCenter_SERVICE_EXCEPTION_AND_DELETE_ERROR(StringRedisTemplate dataSource) {
      return new RedisStateCenter_SERVICE_EXCEPTION_AND_DELETE_ERROR(dataSource);
    }

    @Bean
    public RedisStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_CONSUMING RedisStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_CONSUMING(StringRedisTemplate dataSource) {
      return new RedisStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_CONSUMING(dataSource);
    }

    @Bean
    public RedisStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_EXCEPTION RedisStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_EXCEPTION(StringRedisTemplate dataSource) {
      return new RedisStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_EXCEPTION(dataSource);
    }

    @Bean
    public RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_SUCCESS RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_SUCCESS(StringRedisTemplate dataSource) {
      return new RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_SUCCESS(dataSource);
    }

    @Bean
    public RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_CONSUMING RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_CONSUMING(StringRedisTemplate dataSource) {
      return new RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_CONSUMING(dataSource);
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
  public void test_ServiceException() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);

    String rawKey = "ServiceException";
    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.REQUEST)
              .stateCenter("redisStateCenter")
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {
        throw new ServiceException("for purpose! ");
      });
    } catch (ServiceException serviceException) {
      Assert.assertEquals(serviceException.getClass(), ServiceException.class);
    }

    assertStateCenterEquals(dataSource, rawKey, null, null);
  }


  @Test
  public void test_SERVICE_EXCEPTION_AND_DELETE_ERROR() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);

    String rawKey = "RedisStateCenter_SERVICE_EXCEPTION_AND_DELETE_ERROR";
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
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, "CONSUMING");
    }
  }


  @Test
  public void test_BIZCONSUME_THROWABLE() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);
    String rawKey = "BIZCONSUME_THROWABLE";
    try {
      IdempotenceAttr build = IdempotenceAttr.builder()
              .prefix(prefix)
              .spEL(rawKey)
              .scenario(IdempotenceScenario.MQ)
              .stateCenter("redisStateCenter")
              .consumeMode(ConsumeMode.SUCCESS)
              .build();
      idempotenceHelper.idempotent(build, () -> {
        throw new IllegalArgumentException("for purpose!");
      });
    } catch (IllegalArgumentException illegalArgumentException) {
      Assert.assertEquals(IllegalArgumentException.class, illegalArgumentException.getClass());
    }

    assertStateCenterEquals(dataSource, rawKey, null, "EXCEPTION");
  }


  @Test
  public void test_RedisStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_CONSUMING() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);
    String rawKey = "RedisStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_CONSUMING";

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
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, "CONSUMING");
    }
  }


  @Test
  public void test_RedisStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_EXCEPTION() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);
    String rawKey = "RedisStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_EXCEPTION";

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
      assertStateCenterEquals(dataSource, rawKey, idempotenceException, "EXCEPTION");
    }
  }


  @Test
  public void test_RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_CONSUMING() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);
    String rawKey = "RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_CONSUMING";

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

    assertStateCenterEquals(dataSource, rawKey, null, "CONSUMING");
  }


  @Test
  public void test_RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_SUCCESS() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);
    String rawKey = "RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_SUCCESS";

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
    assertStateCenterEquals(dataSource, rawKey, null, "SUCCESS");
  }


  @Test
  public void test_RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_SUCCESS() throws InterruptedException {
    IdempotenceHelper idempotenceHelper = context.getBean(IdempotenceHelper.class);
    StringRedisTemplate dataSource = context.getBean(StringRedisTemplate.class);
    String rawKey = "RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_SUCCESS";

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
  }


  @After
  public void afterClass() {
    context.close();
  }

}
