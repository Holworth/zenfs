#pragma once

#include <algorithm>
#include <deque>
#include <mutex>
#include <set>
#include <thread>

//
// This iterator provides the functionality of enumerating all possible
// k-elements combination of an input set. The constructor requires an input
// set as base set, and a parameter k as the number of elements to pick.
//
// Once being constructed, call Next() to iteratively get the possible
// combination until Valid() returns false.
//
// Since we directly return the combination as a copy of the base set. We
// require the type parameter T to be copyable. For non-copyable types, we
// suggest the input base set stores the pointer to the actual object.
//
template <typename T>
struct CombinationIterator {
  // The iterator actually iterates on a bitstring of length n, where n is the
  // size of input set. Thus we use a vector to conveniently indexing these
  // elements
  std::vector<T> input_;
  int k_;
  std::string bitmask_;
  bool valid_;

  CombinationIterator(const std::set<T>& input, int k)
      : input_(input.begin(), input.end()),
        k_(k),
        bitmask_(k, 1),
        valid_(true) {
    bitmask_.resize(input.size(), 0);
  }
  ~CombinationIterator() = default;

  // Reset the status of this enumerate iterator
  std::set<T> Next() {
    std::set<T> ret;
    for (int i = 0; i < bitmask_.size(); ++i) {
      if (bitmask_[i]) ret.insert(input_[i]);
    }
    if (!std::prev_permutation(bitmask_.begin(), bitmask_.end())) {
      valid_ = false;
    }
    return ret;
  };

  bool Valid() const { return valid_; }
};

inline uint64_t round_up_align(uint64_t d, uint64_t align) {
  return (d - 1) / align * align + align;
}

inline uint64_t round_down_align(uint64_t d, uint64_t align) {
  return d / align * align;
}

