package cn.addenda.component.idempotence;

import cn.addenda.component.base.exception.SystemException;
import cn.addenda.component.base.string.Slf4jUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/**
 * @author addenda
 * @since 2023/7/29 19:07
 */
public class IdempotenceException extends SystemException {

  @Getter
  @Setter(AccessLevel.PRIVATE)
  private IdempotenceKey idempotenceKey;

  @Getter
  @Setter(AccessLevel.PRIVATE)
  private ConsumeStage consumeStage;

  @Getter
  @Setter(AccessLevel.PRIVATE)
  private String xId;

  public IdempotenceException() {
  }

  public IdempotenceException(String message, ConsumeStage consumeStage) {
    super(message);
    this.consumeStage = consumeStage;
  }

  public IdempotenceException(String message, IdempotenceKey idempotenceKey, ConsumeStage consumeStage, String xId, Throwable cause) {
    super(message, cause);
    this.idempotenceKey = idempotenceKey;
    this.consumeStage = consumeStage;
    this.xId = xId;
  }

  @Override
  public String moduleName() {
    return "idempotence";
  }

  @Override
  public String componentName() {
    return "idempotence";
  }

  @Override
  public String getMessage() {
    return Slf4jUtils.format("idempotenceKey[{}], xId[{}], consumeStage[{}]. ", idempotenceKey, xId, consumeStage) + super.getMessage();
  }

  public static IdempotenceException of(String xId, IdempotenceKey idempotenceKey, ConsumeStage consumeStage) {
    IdempotenceException idempotenceException = new IdempotenceException();
    idempotenceException.setIdempotenceKey(idempotenceKey);
    idempotenceException.setXId(xId);
    idempotenceException.setConsumeStage(consumeStage);
    return idempotenceException;
  }

}
