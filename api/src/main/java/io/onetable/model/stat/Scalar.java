package io.onetable.model.stat;

import lombok.Value;

@Value
public class Scalar implements Range {
  Object value;

  @Override
  public Object getMinValue() {
    return value;
  }

  @Override
  public Object getMaxValue() {
    return value;
  }
}
