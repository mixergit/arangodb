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
/// @author Frank Celler
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

#ifndef ARANGODB_SIMPLE_HTTP_CLIENT_COMMUNICATOR_H
#define ARANGODB_SIMPLE_HTTP_CLIENT_COMMUNICATOR_H 1

#include "curl/curl.h"

#include "Basics/Common.h"
#include "Basics/Mutex.h"

#include "Rest/GeneralRequest.h"
#include "Rest/HttpResponse.h"
#include "SimpleHttpClient/CommunicatorCommon.h"
#include "SimpleHttpClient/CommunicatorThread.h"
#include "SimpleHttpClient/Callbacks.h"
#include "SimpleHttpClient/Destination.h"
#include "SimpleHttpClient/Options.h"

namespace arangodb {
namespace communicator {

class Communicator {
 public:
  Communicator();
  ~Communicator();

 public:
  Ticket addRequest(Destination const&, std::unique_ptr<GeneralRequest>, Callbacks const&, Options const&);

  void abortRequest(Ticket ticketId);
  void abortRequests();
  void disable() { _enabled = false; };
  void enable()  { _enabled = true; };
  void stop() {
    _ioService->stop();
  }


 private:
  bool _enabled;
  std::shared_ptr<boost::asio::io_service> _ioService;
  boost::asio::io_service::work _work;
  CommunicatorThread _communicatorThread;

 private:
  void abortRequestInternal(Ticket ticketId);
  std::vector<RequestInProgress const*> requestsInProgress();
  void transformResult(CURL*, HeadersInProgress&&,
                       std::unique_ptr<basics::StringBuffer>, HttpResponse*);
};
}
}

#endif
