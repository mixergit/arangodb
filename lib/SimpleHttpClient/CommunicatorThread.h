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
/// Asio code based on https://curl.haxx.se/libcurl/c/asiohiper.html
///
/// License below
///
/// @author Andreas Streichardt
////////////////////////////////////////////////////////////////////////////////
/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) 2012 - 2017, Daniel Stenberg, <daniel@haxx.se>, et al.
 *
 * This software is licensed as described in the file COPYING, which
 * you should have received as part of this distribution. The terms
 * are also available at https://curl.haxx.se/docs/copyright.html.
 *
 * You may opt to use, copy, modify, merge, publish, distribute and/or sell
 * copies of the Software, and permit persons to whom the Software is
 * furnished to do so, under the terms of the COPYING file.
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY
 * KIND, either express or implied.
 *
 ***************************************************************************/

#ifndef ARANGODB_SIMPLE_HTTP_CLIENT_COMMUNICATOR_THREAD_H
#define ARANGODB_SIMPLE_HTTP_CLIENT_COMMUNICATOR_THREAD_H 1

#include "Basics/Thread.h"

#include "Basics/Mutex.h"
#include "Basics/StringBuffer.h"
#include "Rest/GeneralRequest.h"
#include "Rest/HttpRequest.h"
#include "Rest/HttpResponse.h"
#include "SimpleHttpClient/CommunicatorCommon.h"
#include "SimpleHttpClient/Callbacks.h"
#include "SimpleHttpClient/Destination.h"
#include "SimpleHttpClient/Options.h"

#include <memory>
#include <boost/asio.hpp>

using namespace arangodb;

namespace arangodb {
namespace communicator {

#ifdef MAINTAINER_MODE
const static double CALLBACK_WARN_TIME = 0.01;
#else
const static double CALLBACK_WARN_TIME = 0.1;
#endif

typedef std::unordered_map<std::string, std::string> HeadersInProgress;

struct RequestInProgress {
  RequestInProgress(Destination const& destination, Callbacks const& callbacks,
                    Ticket const& ticketId, std::string const& requestBody,
                    Options const& options)
      : _destination(destination),
        _callbacks(callbacks),
        _ticketId(ticketId),
        _requestBody(requestBody),
        _requestHeaders(nullptr),
        _startTime(0.0),
        _responseBody(new basics::StringBuffer(false)),
        _options(options),
        _aborted(false) {
    _errorBuffer[0] = '\0';
  }

  ~RequestInProgress() {
    if (_requestHeaders != nullptr) {
      curl_slist_free_all(_requestHeaders);
    }
  }

  RequestInProgress(RequestInProgress const& other) = delete;
  RequestInProgress& operator=(RequestInProgress const& other) = delete;

  // mop: i think we should just hold the full request here later
  Destination _destination;
  Callbacks _callbacks;
  Ticket _ticketId;
  std::string _requestBody;
  struct curl_slist* _requestHeaders;

  HeadersInProgress _responseHeaders;
  double _startTime;
  std::unique_ptr<basics::StringBuffer> _responseBody;
  Options _options;

  char _errorBuffer[CURL_ERROR_SIZE];
  bool _aborted;
};

struct CurlHandle {
  explicit CurlHandle(CURL* handle, RequestInProgress* rip) : _handle(handle), _rip(rip) {
    curl_easy_setopt(_handle, CURLOPT_PRIVATE, _rip.get());
  }
  ~CurlHandle() {
    if (_handle != nullptr) {
      curl_easy_cleanup(_handle);
    }
  }

  CurlHandle(CurlHandle& other) = delete;
  CurlHandle& operator=(CurlHandle& other) = delete;

  CURL* _handle;
  std::unique_ptr<RequestInProgress> _rip;
};

struct NewRequest {
  NewRequest(
    Destination const& destination,
    GeneralRequest* request,
    Callbacks const& callbacks,
    Options const& options,
    Ticket const& ticket
  ) : _destination(destination),
      _request(request),
      _callbacks(callbacks),
      _options(options),
      _ticketId(ticket) {
  }

  Destination _destination;
  std::unique_ptr<GeneralRequest> _request;
  Callbacks _callbacks;
  Options _options;
  Ticket _ticketId;
};

class CommunicatorThread: public Thread {
 public:
  CommunicatorThread(
    std::shared_ptr<boost::asio::io_service> ioService
  ) : Thread("Communicator"),
      _ioService(ioService),
      _curl(nullptr),
      _timer(*_ioService) {
  }
  ~CommunicatorThread() {
    if (_curl != nullptr) {
      ::curl_multi_cleanup(_curl);
    }
    shutdown();
  };
 
 public:
  void createRequest(Ticket const& id, Destination const& destination,
                                GeneralRequest* requestPtr,
                                Callbacks const& callbacks, Options const& options);
  virtual void run() override;
  virtual bool isSystem() override {
    return true;
  }

 private:
  std::string createSafeDottedCurlUrl(std::string const& originalUrl);
  // multi callbacks
  static int sockCb(CURL *e, curl_socket_t s, int what, void *cbp, void *sockp);
  static int curlTimerCb(CURLM *multi, long timeout_ms, void*);

  void addSocket(curl_socket_t s, CURL *easy, int action);
  void setSocket(int *fdp, curl_socket_t s, CURL *e, int act, int oldact);
  void removeSocket(int *f);


  // easy callbacks
  static curl_socket_t openSocket(void *clientp, curlsocktype purpose, struct curl_sockaddr *address);
  static int closeSocket(void *clientp, curl_socket_t item);
  
  static size_t readBody(void*, size_t, size_t, void*);
  static size_t readHeaders(char* buffer, size_t size, size_t nitems,
                            void* userdata);
  static void logHttpHeaders(std::string const&, std::string const&);
  static void logHttpBody(std::string const&, std::string const&);
  static int curlDebug(CURL*, curl_infotype, char*, size_t, void*);
  static int curlProgress(void*, curl_off_t, curl_off_t, curl_off_t, curl_off_t);
  // boost cbs
  void boostTimerCb(boost::system::error_code const& error);
  void eventCb(curl_socket_t s,
    int action, const boost::system::error_code & error,
    int *fdp);
  
  int handleMultiSocket(curl_socket_t s, int const& action);
  void processResult(CURL* handle, CURLcode rc);
  void callResultCallback(int const& errorNumber,
    Ticket const& ticket,
    Destination const& destination,
    Callbacks const& callbacks,
    std::unique_ptr<HttpResponse> response
  );

  CURL* createPrototype();
 
 private:
  std::shared_ptr<boost::asio::io_service> _ioService;
  CURLM* _curl;
  boost::asio::deadline_timer _timer;
  std::map<curl_socket_t, boost::asio::ip::tcp::socket *> _socketMap;
  Mutex _handlesLock;
  std::unordered_map<uint64_t, std::unique_ptr<CurlHandle>> _handlesInProgress;
};

}
}

#endif