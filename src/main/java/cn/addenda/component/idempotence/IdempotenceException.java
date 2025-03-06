package cn.addenda.component.idempotence;

import cn.addenda.component.base.exception.SystemException;
import lombok.Getter;

/**
 * @author addenda
 * @since 2023/7/29 19:07
 */
public class IdempotenceException extends SystemException {

  @Getter
  private ConsumeStage consumeStage;

  @Getter
  private String xId;

  public IdempotenceException(String message, ConsumeStage consumeStage) {
    super(message);
    this.consumeStage = consumeStage;
  }

  public IdempotenceException(String message, ConsumeStage consumeStage, String xId, Throwable cause) {
    super(message, cause);
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
    return "xId[" + xId + "]. " + super.getMessage();
  }
}
