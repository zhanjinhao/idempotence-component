package cn.addenda.component.idempotence.statecenter;

import cn.addenda.component.base.string.StringUtils;
import cn.addenda.component.idempotence.ConsumeMode;
import cn.addenda.component.idempotence.ConsumeState;
import cn.addenda.component.idempotence.IdempotenceScenario;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;

/**
 * @author addenda
 * @since 2023/9/15 9:08
 */
@Setter
@Getter
@ToString
public class StateCenterEntity {

  private Long id;

  private String namespace;

  private String prefix;

  private String rawKey;

  private ConsumeMode consumeMode;

  private String xId;

  private IdempotenceScenario scenario;

  private ConsumeState consumeState;

  private LocalDateTime expireTime;

  private LocalDateTime createTime;

  public String getKey() {
    return format(namespace) + ":" + format(getPrefix()) + ":" + format(rawKey);
  }

  public String getSimpleKey() {
    return format(getPrefix()) + ":" + format(rawKey);
  }

  private String format(String a) {
    return StringUtils.biTrimSpecifiedChar(a, ':');
  }
}
