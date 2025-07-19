package cn.addenda.component.idempotence.test.redis;

import cn.addenda.component.idempotence.ConsumeState;
import cn.addenda.component.idempotence.IdempotenceException;
import cn.addenda.component.idempotence.IdempotenceParamWrapper;
import cn.addenda.component.idempotence.statecenter.RedisStateCenter;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.springframework.data.redis.core.StringRedisTemplate;

public abstract class AbstractIdempotenceRedisTest {

  static class RedisStateCenter_BEFORE_CONSUMPTION extends RedisStateCenter {
    public RedisStateCenter_BEFORE_CONSUMPTION(StringRedisTemplate dataSource) {
      super(dataSource);
    }

    @Override
    public ConsumeState getSetIfAbsent(IdempotenceParamWrapper param, ConsumeState consumeState) {
      throw new RuntimeException("getSetIfAbsent");
    }
  }

  static class RedisStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_NULL extends RedisStateCenter {
    public RedisStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_NULL(StringRedisTemplate dataSource) {
      super(dataSource);
    }

    @Override
    public ConsumeState getSetIfAbsent(IdempotenceParamWrapper param, ConsumeState consumeState) {
      super.getSetIfAbsent(param, consumeState);
      throw new RuntimeException("getSetIfAbsent");
    }

    @Override
    public boolean delete(IdempotenceParamWrapper param) {
      super.delete(param);
      throw new RuntimeException("delete");
    }
  }

  static class RedisStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_CONSUMING extends RedisStateCenter {
    public RedisStateCenter_GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR_CONSUMING(StringRedisTemplate dataSource) {
      super(dataSource);
    }

    @Override
    public ConsumeState getSetIfAbsent(IdempotenceParamWrapper param, ConsumeState consumeState) {
      super.getSetIfAbsent(param, consumeState);
      throw new RuntimeException("getSetIfAbsent");
    }

    @Override
    public boolean delete(IdempotenceParamWrapper param) {
      throw new RuntimeException("delete");
    }
  }

  static class RedisStateCenter_SERVICE_EXCEPTION_AND_DELETE_ERROR_CONSUMING extends RedisStateCenter {
    public RedisStateCenter_SERVICE_EXCEPTION_AND_DELETE_ERROR_CONSUMING(StringRedisTemplate dataSource) {
      super(dataSource);
    }

    @Override
    public boolean delete(IdempotenceParamWrapper param) {
      throw new RuntimeException("delete");
    }
  }

  static class RedisStateCenter_SERVICE_EXCEPTION_AND_DELETE_ERROR_NULL extends RedisStateCenter {
    public RedisStateCenter_SERVICE_EXCEPTION_AND_DELETE_ERROR_NULL(StringRedisTemplate dataSource) {
      super(dataSource);
    }

    @Override
    public boolean delete(IdempotenceParamWrapper param) {
      super.delete(param);
      throw new RuntimeException("delete");
    }
  }

  static class RedisStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_CONSUMING extends RedisStateCenter {
    public RedisStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_CONSUMING(StringRedisTemplate dataSource) {
      super(dataSource);
    }

    @Override
    public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
      if (expected == ConsumeState.CONSUMING && consumeState == ConsumeState.EXCEPTION) {
        throw new RuntimeException("for purpose! ");
      }
      return super.casState(param, expected, consumeState, casOther);
    }
  }

  static class RedisStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_EXCEPTION extends RedisStateCenter {
    public RedisStateCenter_CAS_CONSUMING_TO_EXCEPTION_ERROR_EXCEPTION(StringRedisTemplate dataSource) {
      super(dataSource);
    }

    @Override
    public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
      boolean b = super.casState(param, expected, consumeState, casOther);
      if (expected == ConsumeState.CONSUMING && consumeState == ConsumeState.EXCEPTION) {
        throw new RuntimeException("for purpose! ");
      }
      return b;
    }
  }

  static class RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_SUCCESS extends RedisStateCenter {
    public RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_SUCCESS(StringRedisTemplate dataSource) {
      super(dataSource);
    }

    @Override
    public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
      boolean b = super.casState(param, expected, consumeState, casOther);
      if (expected == ConsumeState.CONSUMING && consumeState == ConsumeState.SUCCESS) {
        throw new RuntimeException("for purpose! ");
      }
      return b;
    }
  }

  static class RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_CONSUMING extends RedisStateCenter {
    public RedisStateCenter_CAS_CONSUMING_TO_SUCCESS_ERROR_CONSUMING(StringRedisTemplate dataSource) {
      super(dataSource);
    }

    @Override
    public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
      if (expected == ConsumeState.CONSUMING && consumeState == ConsumeState.SUCCESS) {
        throw new RuntimeException("for purpose! ");
      }
      return super.casState(param, expected, consumeState, casOther);
    }
  }

  static class RedisStateCenter_RETRY_ERROR extends RedisStateCenter {
    public RedisStateCenter_RETRY_ERROR(StringRedisTemplate dataSource) {
      super(dataSource);
    }

    @Override
    public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
      boolean b = super.casState(param, expected, consumeState, casOther);
      if (expected == ConsumeState.EXCEPTION && consumeState == ConsumeState.CONSUMING) {
        throw new RuntimeException("for purpose! ");
      }
      return b;
    }
  }

  static class RedisStateCenter_RETRY_ERROR_AND_RESET_ERROR_EXCEPTION extends RedisStateCenter {
    public RedisStateCenter_RETRY_ERROR_AND_RESET_ERROR_EXCEPTION(StringRedisTemplate dataSource) {
      super(dataSource);
    }

    @Override
    public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
      boolean b = super.casState(param, expected, consumeState, casOther);
      if (expected == ConsumeState.EXCEPTION && consumeState == ConsumeState.CONSUMING) {
        throw new RuntimeException("for purpose! ");
      }
      if (expected == ConsumeState.CONSUMING && consumeState == ConsumeState.EXCEPTION) {
        throw new RuntimeException("for purpose! ");
      }
      return b;
    }
  }

  static class RedisStateCenter_RETRY_ERROR_AND_RESET_ERROR_CONSUMING extends RedisStateCenter {
    public RedisStateCenter_RETRY_ERROR_AND_RESET_ERROR_CONSUMING(StringRedisTemplate dataSource) {
      super(dataSource);
    }

    @Override
    public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
      if (expected == ConsumeState.CONSUMING && consumeState == ConsumeState.EXCEPTION) {
        throw new RuntimeException("for purpose! ");
      }
      boolean b = super.casState(param, expected, consumeState, casOther);
      if (expected == ConsumeState.EXCEPTION && consumeState == ConsumeState.CONSUMING) {
        throw new RuntimeException("for purpose! ");
      }
      return b;
    }
  }


  @SneakyThrows
  protected void assertStateCenterEquals(StringRedisTemplate dataSource, String rawKey, IdempotenceException idempotenceException,
                                         String expected) {

    String consumeState = (String) dataSource.opsForHash().get("idempotence:prefix:" + rawKey, "consumeState");
    String xId = (String) dataSource.opsForHash().get("idempotence:prefix:" + rawKey, "xId");
    if (idempotenceException != null) {
      Assert.assertEquals(idempotenceException.getXId(), xId);
    }

    Assert.assertEquals(expected, consumeState);
  }

}
