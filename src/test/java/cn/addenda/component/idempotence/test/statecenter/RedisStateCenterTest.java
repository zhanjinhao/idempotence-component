package cn.addenda.component.idempotence.test.statecenter;

import cn.addenda.component.base.exception.ServiceException;
import cn.addenda.component.idempotence.*;
import cn.addenda.component.idempotence.statecenter.RedisStateCenter;
import cn.addenda.component.idempotence.test.IdempotenceTestConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Set;

/**
 * @author addenda
 * @since 2023/9/17 12:34
 */
@Slf4j
public class RedisStateCenterTest {

  static AnnotationConfigApplicationContext context;

  private static final String namespace = "idempotence-component";
  private static final String prefix = "prefix";

  @BeforeClass
  public static void beforeClass() {
    context = new AnnotationConfigApplicationContext();
    context.register(IdempotenceTestConfiguration.class);
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
  public void test() {
    test1();
    test2();
    test3();
    test4();
    test5();
    test6();
    test7();
    test8();
    test9();
  }

  public void test1() {
    RedisStateCenter redisStateCenter = context.getBean(RedisStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("1");
    build.setTtlInSecs(10000);
    ConsumeState set = redisStateCenter.getSetIfAbsent(build, ConsumeState.CONSUMING);
    Assert.assertNull(set);
    log.info("GetSetIfAbsent: {}", set);
  }

  public void test2() {
    RedisStateCenter redisStateCenter = context.getBean(RedisStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("1");
    build.setTtlInSecs(10000);
    ConsumeState set = redisStateCenter.getSetIfAbsent(build, ConsumeState.CONSUMING);
    Assert.assertEquals(ConsumeState.CONSUMING, set);
    log.info("GetSetIfAbsent: {}", ConsumeState.CONSUMING);
  }

  public void test3() {
    RedisStateCenter redisStateCenter = context.getBean(RedisStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("1");
    build.setTtlInSecs(10000);
    boolean b = redisStateCenter.casState(build, ConsumeState.CONSUMING, ConsumeState.SUCCESS, false);
    log.info("casState: {}", b);
    Assert.assertTrue(b);
  }

  public void test4() {
    RedisStateCenter redisStateCenter = context.getBean(RedisStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("1");
    build.setTtlInSecs(10000);
    boolean b = redisStateCenter.casState(build, ConsumeState.CONSUMING, ConsumeState.SUCCESS, false);
    log.info("casState: {}", b);
    Assert.assertFalse(b);
  }

  public void test5() {
    RedisStateCenter redisStateCenter = context.getBean(RedisStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("2");
    build.setTtlInSecs(10000);
    boolean b = redisStateCenter.casState(build, ConsumeState.SUCCESS, ConsumeState.CONSUMING, false);
    log.info("casState: {}", b);
    Assert.assertFalse(b);
  }

  public void test6() {
    RedisStateCenter redisStateCenter = context.getBean(RedisStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("2");
    build.setTtlInSecs(10000);
    boolean b = redisStateCenter.casState(build, ConsumeState.SUCCESS, ConsumeState.CONSUMING, true);
    log.info("casState: {}", b);
    Assert.assertTrue(b);
  }

  public void test7() {
    RedisStateCenter redisStateCenter = context.getBean(RedisStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("1");
    build.setTtlInSecs(10000);
    boolean delete = redisStateCenter.delete(build);
    Assert.assertFalse(delete);
  }

  public void test8() {
    RedisStateCenter redisStateCenter = context.getBean(RedisStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("2");
    build.setTtlInSecs(10000);
    boolean delete = redisStateCenter.delete(build);
    Assert.assertTrue(delete);
  }


  public void test9() {
    RedisStateCenter redisStateCenter = context.getBean(RedisStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("2");
    build.setTtlInSecs(10000);
    redisStateCenter.saveExceptionLog(build, new Object[]{build},
            ConsumeStage.BEFORE_CONSUMPTION, "test", new ServiceException());
  }

  @AfterClass
  public static void afterClass() {
    context.close();
  }

}
