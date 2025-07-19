package cn.addenda.component.idempotence.test;

import cn.addenda.component.idempotence.ConsumeStage;
import cn.addenda.component.idempotence.IdempotenceException;
import cn.addenda.component.idempotence.IdempotenceKey;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.UUID;

@Slf4j
public class IdempotenceExceptionTest {

  @Test
  public void test1() {
    IdempotenceException idempotenceException = IdempotenceException.of(
            UUID.randomUUID().toString().replace("-", ""),
            IdempotenceKey.of("a", "b", "c"),
            ConsumeStage.RETRY_ERROR_AND_RESET_ERROR);
    log.info("", idempotenceException);
  }

}
