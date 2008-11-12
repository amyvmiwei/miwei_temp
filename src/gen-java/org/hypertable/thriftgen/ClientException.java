/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.hypertable.thriftgen;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import com.facebook.thrift.*;

import com.facebook.thrift.protocol.*;
import com.facebook.thrift.transport.*;

public class ClientException extends Exception implements TBase, java.io.Serializable, Cloneable {
  public int code;
  public static final int CODE = 1;
  public String what;
  public static final int WHAT = 2;

  public final Isset __isset = new Isset();
  public static final class Isset implements java.io.Serializable {
    public boolean code = false;
    public boolean what = false;
  }

  public ClientException() {
  }

  public ClientException(
    int code,
    String what)
  {
    this();
    this.code = code;
    this.__isset.code = true;
    this.what = what;
    this.__isset.what = (what != null);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ClientException(ClientException other) {
    __isset.code = other.__isset.code;
    this.code = other.code;
    __isset.what = other.__isset.what;
    if (other.what != null) {
      this.what = other.what;
    }
  }

  public ClientException clone() {
    return new ClientException(this);
  }

  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ClientException)
      return this.equals((ClientException)that);
    return false;
  }

  public boolean equals(ClientException that) {
    if (that == null)
      return false;

    boolean this_present_code = true;
    boolean that_present_code = true;
    if (this_present_code || that_present_code) {
      if (!(this_present_code && that_present_code))
        return false;
      if (this.code != that.code)
        return false;
    }

    boolean this_present_what = true && (this.what != null);
    boolean that_present_what = true && (that.what != null);
    if (this_present_what || that_present_what) {
      if (!(this_present_what && that_present_what))
        return false;
      if (!this.what.equals(that.what))
        return false;
    }

    return true;
  }

  public int hashCode() {
    return 0;
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id)
      {
        case CODE:
          if (field.type == TType.I32) {
            this.code = iprot.readI32();
            this.__isset.code = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case WHAT:
          if (field.type == TType.STRING) {
            this.what = iprot.readString();
            this.__isset.what = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
          break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();

    // check for required fields
  }

  public void write(TProtocol oprot) throws TException {


    TStruct struct = new TStruct("ClientException");
    oprot.writeStructBegin(struct);
    TField field = new TField();
    field.name = "code";
    field.type = TType.I32;
    field.id = CODE;
    oprot.writeFieldBegin(field);
    oprot.writeI32(this.code);
    oprot.writeFieldEnd();
    if (this.what != null) {
      field.name = "what";
      field.type = TType.STRING;
      field.id = WHAT;
      oprot.writeFieldBegin(field);
      oprot.writeString(this.what);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("ClientException(");
    boolean first = true;

    if (!first) sb.append(", ");
    sb.append("code:");
    sb.append(this.code);
    first = false;
    if (!first) sb.append(", ");
    sb.append("what:");
    sb.append(this.what);
    first = false;
    sb.append(")");
    return sb.toString();
  }

}

