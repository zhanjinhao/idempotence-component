package cn.addenda.component.idempotence;

import cn.addenda.component.base.string.StringUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Objects;

@Setter
@Getter
@ToString
public class IdempotenceKey {

  private String namespace;

  private String prefix;

  private String rawKey;

  public IdempotenceKey() {
  }

  public IdempotenceKey(String namespace, String prefix, String rawKey) {
    this.namespace = namespace;
    this.prefix = prefix;
    this.rawKey = rawKey;
  }

  public String getKey() {
    return format(namespace) + ":" + format(prefix) + ":" + format(rawKey);
  }

  public String getSimpleKey() {
    return format(prefix) + ":" + format(rawKey);
  }

  private String format(String a) {
    return StringUtils.biTrimSpecifiedChar(a, ':');
  }

  public static IdempotenceKey of(String namespace, String prefix, String rawKey) {
    return new IdempotenceKey(namespace, prefix, rawKey);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IdempotenceKey that = (IdempotenceKey) o;
    return Objects.equals(namespace, that.namespace) && Objects.equals(prefix, that.prefix)
            && Objects.equals(rawKey, that.rawKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, prefix, rawKey);
  }

}
