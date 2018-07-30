/*******************************************************************************
 * Copyright (c) 2013, 2015 EclipseSource.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 ******************************************************************************/
package com.hazelcast.internal.json;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializableByConvention;

import java.io.IOException;

@SuppressWarnings("serial") // use default serial UID
@SerializableByConvention
public class JsonLiteral extends JsonValue {

  private String value;
  private boolean isNull;
  private boolean isTrue;
  private boolean isFalse;

  public JsonLiteral() {
    //use during deserialization only
  }

  public JsonLiteral(String value) {
    this.value = value;
    isNull = "null".equals(value);
    isTrue = "true".equals(value);
    isFalse = "false".equals(value);
  }

  @Override
  void write(JsonWriter writer) throws IOException {
    writer.writeLiteral(value);
  }

  @Override
  public String toString() {
    return value;
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public boolean isNull() {
    return isNull;
  }

  @Override
  public boolean isTrue() {
    return isTrue;
  }

  @Override
  public boolean isFalse() {
    return isFalse;
  }

  @Override
  public boolean isBoolean() {
    return isTrue || isFalse;
  }

  @Override
  public boolean asBoolean() {
    return isNull ? super.asBoolean() : isTrue;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null) {
      return false;
    }
    if (getClass() != object.getClass()) {
      return false;
    }
    JsonLiteral other = (JsonLiteral)object;
    return value.equals(other.value);
  }

  @Override
  public void writeData(ObjectDataOutput out) throws IOException {
    out.writeUTF(value);
    out.writeBoolean(isFalse);
    out.writeBoolean(isNull);
    out.writeBoolean(isTrue);
  }

  @Override
  public void readData(ObjectDataInput in) throws IOException {
    value = in.readUTF();
    isFalse = in.readBoolean();
    isNull = in.readBoolean();
    isTrue = in.readBoolean();
  }

  @Override
  public int getFactoryId() {
    return JsonFactoryHook.F_ID;
  }

  @Override
  public int getId() {
    return JsonFactoryHook.LITERAL;
  }
}
