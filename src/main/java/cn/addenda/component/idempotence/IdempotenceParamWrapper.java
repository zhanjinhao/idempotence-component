package cn.addenda.component.idempotence;

import cn.addenda.component.base.string.StringUtils;
import lombok.*;

/**
 * @author addenda
 * @since 2023/7/29 18:07
 */
@Setter
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class IdempotenceParamWrapper extends IdempotenceAttr {

  private String namespace;

  private String rawKey;

  private String xId;

  /**
   * 此参数是由{@link IdempotenceAttr#getTimeUnit()}和@{@link IdempotenceAttr#getTtl()}构成。
   * 单独设立一个参数为了后续便于使用
   */
  private int ttlInSecs;

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
