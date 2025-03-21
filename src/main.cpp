#include <iostream>
#include <string>
#include "stx/btree_map.h"

int main() {
  stx::btree_map<int, std::string> map;
  for (int i = 0; i < 100; i++) {
    map[i] = "Hello";
  }
  for (auto it = map.begin(); it != map.end(); ++it) {
    std::cout << it->first << " " << it->second << std::endl;
  }

  int *p = new int(10);

  return 0;
}