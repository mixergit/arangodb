////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Simon Grätzer
////////////////////////////////////////////////////////////////////////////////

#include <cstdint>
#include <string>

#include <velocypack/Builder.h>
#include <velocypack/Slice.h>
#include <velocypack/velocypack-aliases.h>

#ifndef ARANGODB_PREGEL_AGGREGATOR_H
#define ARANGODB_PREGEL_AGGREGATOR_H 1

namespace arangodb {
namespace pregel {

class Aggregator {
  bool _permanent;
  Aggregator(const Aggregator&) = delete;
  Aggregator& operator=(const Aggregator&) = delete;

 public:
  Aggregator(bool perm = false) : _permanent(perm) {}
  virtual ~Aggregator() {}

  /// @brief Value from superstep S-1 supplied by the conductor
  virtual void aggregate(const void* valuePtr) = 0;
  virtual void aggregate(VPackSlice slice) = 0;

  virtual void const* getValue() const = 0;
  // virtual void setValue(VPackSlice slice) = 0;
  virtual VPackValue vpackValue() = 0;

  virtual void reset(){};
  bool isPermanent() { return _permanent; }
};

class FloatMaxAggregator : public Aggregator {
  float _value, _initial;

 public:
  FloatMaxAggregator(float init) : _value(init), _initial(init) {}

  void aggregate(void const* valuePtr) override {
    float other = *((float*)valuePtr);
    if (other > _value) _value = other;
  };
  void aggregate(VPackSlice slice) override {
    float f = slice.getNumber<float>();
    aggregate(&f);
  }

  void const* getValue() const override { return &_value; };
  /*void setValue(VPackSlice slice) override {
    _value = (float)slice.getDouble();
  }*/
  VPackValue vpackValue() override { return VPackValue((double)_value); };

  void reset() override { _value = _initial; }
};

template<typename T>
class MinAggregator : public Aggregator {
  static_assert(std::is_arithmetic<T>::value, "Type must be numeric");
  T _value, _initial;
  
public:
  MinAggregator(T init) : _value(init), _initial(init) {}
  
  void aggregate(void const* valuePtr) override {
    T other = *((T*)valuePtr);
    if (other < _value) _value = other;
  };
  void aggregate(VPackSlice slice) override {
    T f = slice.getNumber<T>();
    aggregate(&f);
  }
  
  void const* getValue() const override { return &_value; };
  /*void setValue(VPackSlice slice) override {
   _value = (float)slice.getDouble();
   }*/
  VPackValue vpackValue() override { return VPackValue(_value); };
  
  void reset() override { _value = _initial; }
};

template <typename T>
class ValueAggregator : public Aggregator {
  static_assert(std::is_arithmetic<T>::value, "Type must be numeric");

  T _value;

 public:
  ValueAggregator(T val) : Aggregator(true), _value(val) {}

  void aggregate(void const* valuePtr) override { _value = *((T*)valuePtr); };
  void aggregate(VPackSlice slice) override { _value = slice.getNumber<T>(); }

  void const* getValue() const override { return &_value; };
  /*void setValue(VPackSlice slice) override {
   _value = (float)slice.getDouble();
   }*/
  VPackValue vpackValue() override { return VPackValue(_value); };
};

template <typename T>
class SumAggregator : public Aggregator {
  static_assert(std::is_arithmetic<T>::value, "Type must be numeric");

  T _value;

 public:
  SumAggregator(T val) : Aggregator(true), _value(val) {}

  void aggregate(void const* valuePtr) override { _value += *((T*)valuePtr); };
  void aggregate(VPackSlice slice) override { _value += slice.getNumber<T>(); }

  void const* getValue() const override { return &_value; };
  /*void setValue(VPackSlice slice) override {
   _value = (float)slice.getDouble();
   }*/
  VPackValue vpackValue() override { return VPackValue(_value); };
};
}
}
#endif
