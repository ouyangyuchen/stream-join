#include <iostream>
#include <thread>
#include "utils/decorator.hpp"

void myFunction(int x, const std::string &s) {
  std::cout << "myFunction called with x=" << x << ", s=" << s << std::endl;
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

int anotherFunction(int a, int b) {
  std::cout << "anotherFunction called with a=" << a << ", b=" << b << std::endl;
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  return a + b;
}

struct MyFunctor {
  void operator()(double d) const {
    std::cout << "MyFunctor called with d=" << d << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(75));
  }
};

using namespace decorator;

int main() {
  // Decorate the functions/callables
  auto timedMyFunction = decorateWithTimer(myFunction, "myFunctionKey");
  auto timedAnotherFunction = decorateWithTimer(anotherFunction, "anotherFunctionKey");
  auto timedLambda = decorateWithTimer(
      [](const std::vector<int> &v) {
        long long sum = 0;
        for (int x : v) sum += x;
        std::cout << "Lambda called, vector sum = " << sum << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        return sum;
      },
      "lambdaKey");

  MyFunctor myFunctor;
  auto timedFunctor = decorateWithTimer(myFunctor, "functorKey");

  // Call the decorated functions
  std::cout << "Calling decorated functions...\n";
  timedMyFunction(10, ("hello"));
  int result = timedAnotherFunction(5, 3);
  std::cout << "anotherFunction result: " << result << std::endl;
  timedMyFunction(20, "world");  // Call again to accumulate time, explicitly use std::string
  long long lambdaResult = timedLambda(std::vector{1, 2, 3, 4, 5});
  std::cout << "Lambda result: " << lambdaResult << std::endl;
  timedFunctor(3.14);
  timedAnotherFunction(1, 1);  // Call again

  // Print the total durations
  std::cout << "\nRetrieving durations...\n";
  printAllDurations();

  // Retrieve a specific duration
  auto myFuncTotalDuration = getTotalDuration("myFunctionKey");
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(myFuncTotalDuration);
  std::cout << "Total duration specifically for myFunctionKey: " << ms.count() << " ms" << std::endl;

  auto unknownDuration = getTotalDuration("nonExistentKey");
  ms = std::chrono::duration_cast<std::chrono::milliseconds>(unknownDuration);
  std::cout << "Total duration specifically for nonExistentKey: " << ms.count() << " ms" << std::endl;

  return 0;
}