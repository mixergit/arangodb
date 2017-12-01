#include <chrono>
#include <iostream>
#include <thread>

#include "Logger/Logger.h"
#include "Logger/LogAppender.h"
#include "Rest/HttpRequest.h"
#include "SimpleHttpClient/Communicator.h"
#include "SimpleHttpClient/Callbacks.h"
#include "SimpleHttpClient/Destination.h"

using namespace arangodb;
using namespace arangodb::communicator;

int main() {
  arangodb::Logger::initialize(false);
  arangodb::Logger::setLogLevel("communication=trace");
  arangodb::LogAppender::addAppender("-");
  Communicator communicator;

  std::string body("{\"test\": \"test\"}");
  auto start = std::chrono::high_resolution_clock::now();
  communicator::Callbacks callbacks([](std::unique_ptr<GeneralResponse> response) {
    //std::cout << "gmm " << std::endl;
  }, [](int errorCode, std::unique_ptr<GeneralResponse> response) {
    //std::cout << "ERRORCODE " << errorCode << std::endl;
  });
  auto request = std::unique_ptr<HttpRequest>(HttpRequest::createHttpRequest(rest::ContentType::TEXT, "", 0, {}));
  request->setRequestType(RequestType::GET);
  request->setBody(body.c_str(), body.length());
  communicator::Options opt;
  auto destination = Destination("http://suelz:8529/_api/document/test");
  communicator.addRequest(destination, std::move(request), callbacks, opt);
  communicator.work_once();
  // while (communicator.work_once() > 0) {
  //   std::cout << "HA" << std::endl;
  //   std::this_thread::sleep_for(std::chrono::microseconds(1));
  // }
  std::cout << "HMMM" << std::endl;
  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "Elapsed " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << std::endl;
  arangodb::Logger::flush();
  arangodb::Logger::shutdown();
}