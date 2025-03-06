package cn.addenda.component.idempotence.test.statecenter;

import cn.addenda.component.base.exception.ServiceException;
import cn.addenda.component.idempotence.*;
import cn.addenda.component.idempotence.statecenter.DbStateCenter;
import cn.addenda.component.idempotence.test.IdempotenceTestConfiguration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

/**
 * @author addenda
 * @since 2023/9/17 12:34
 */
@Slf4j
public class DbStateCenterTest {

  static AnnotationConfigApplicationContext context;

  private static final String namespace = "idempotence-component";
  private static final String prefix = "prefix";

  @BeforeClass
  @SneakyThrows
  public static void beforeClass() {
    context = new AnnotationConfigApplicationContext();
    context.register(IdempotenceTestConfiguration.class);
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
    DbStateCenter stateCenter = context.getBean(DbStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("1");
    build.setTtlInSecs(10000);
    ConsumeState set = stateCenter.getSetIfAbsent(build, ConsumeState.CONSUMING);
    Assert.assertNull(set);
    log.info("GetSetIfAbsent: {}", set);
  }

  public void test2() {
    DbStateCenter stateCenter = context.getBean(DbStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("1");
    build.setTtlInSecs(10000);
    ConsumeState set = stateCenter.getSetIfAbsent(build, ConsumeState.CONSUMING);
    Assert.assertEquals(ConsumeState.CONSUMING, set);
    log.info("GetSetIfAbsent: {}", ConsumeState.CONSUMING);
  }

  public void test3() {
    DbStateCenter stateCenter = context.getBean(DbStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("1");
    build.setTtlInSecs(10000);
    boolean b = stateCenter.casState(build, ConsumeState.CONSUMING, ConsumeState.SUCCESS, false);
    log.info("casState: {}", b);
    Assert.assertTrue(b);
  }

  public void test4() {
    DbStateCenter stateCenter = context.getBean(DbStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("1");
    build.setTtlInSecs(10000);
    boolean b = stateCenter.casState(build, ConsumeState.CONSUMING, ConsumeState.SUCCESS, false);
    log.info("casState: {}", b);
    Assert.assertFalse(b);
  }

  public void test5() {
    DbStateCenter stateCenter = context.getBean(DbStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("2");
    build.setTtlInSecs(10000);
    boolean b = stateCenter.casState(build, ConsumeState.SUCCESS, ConsumeState.CONSUMING, false);
    log.info("casState: {}", b);
    Assert.assertFalse(b);
  }

  public void test6() {
    DbStateCenter stateCenter = context.getBean(DbStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("2");
    build.setTtlInSecs(10000);
    boolean b = stateCenter.casState(build, ConsumeState.SUCCESS, ConsumeState.CONSUMING, true);
    log.info("casState: {}", b);
    Assert.assertTrue(b);
  }

  public void test7() {
    DbStateCenter stateCenter = context.getBean(DbStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("1");
    build.setTtlInSecs(10000);
    boolean delete = stateCenter.delete(build);
    Assert.assertFalse(delete);
  }

  public void test8() {
    DbStateCenter stateCenter = context.getBean(DbStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("2");
    build.setTtlInSecs(10000);
    boolean delete = stateCenter.delete(build);
    Assert.assertTrue(delete);
  }

  public void test9() {
    DbStateCenter stateCenter = context.getBean(DbStateCenter.class);
    IdempotenceParamWrapper build = new IdempotenceParamWrapper();
    build.setNamespace(namespace);
    build.setPrefix(prefix);
    build.setConsumeMode(ConsumeMode.SUCCESS);
    build.setScenario(IdempotenceScenario.REQUEST);
    build.setRawKey("StateCenterTest");
    build.setXId("2");
    build.setTtlInSecs(10000);
    stateCenter.saveExceptionLog(build, new Object[]{build},
            ConsumeStage.BEFORE_CONSUMPTION, "test", new ServiceException());
  }

  @AfterClass
  public static void afterClass() {
    context.close();
  }

}
