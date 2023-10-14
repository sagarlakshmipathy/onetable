package io.onetable.model.stat;

import lombok.Value;

@Value
public class Vector implements Range {
  Object minValue;
  Object maxValue;
}
