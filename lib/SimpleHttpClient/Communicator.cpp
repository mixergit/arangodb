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
/// @author Andreas Streichardt
/// @author Frank Celler
////////////////////////////////////////////////////////////////////////////////

#include "Communicator.h"

#include <iostream>

#include "Basics/MutexLocker.h"
#include "Basics/socket-utils.h"
#include "Logger/Logger.h"
#include "Rest/HttpRequest.h"

#include <boost/asio.hpp>
#include <boost/bind.hpp>

using namespace arangodb;
using namespace arangodb::basics;
using namespace arangodb::communicator;

namespace {

std::atomic_uint_fast64_t NEXT_TICKET_ID(static_cast<uint64_t>(0));
std::vector<char> urlDotSeparators{'/', '#', '?'};
}

Communicator::Communicator() : _enabled(true),
                               _ioService(std::make_shared<boost::asio::io_service>()),
                               _work(*_ioService),
                               _communicatorThread(_ioService) {
  LOG_TOPIC(WARN, Logger::COMMUNICATION) << "Communicatior initialized";
  curl_global_init(CURL_GLOBAL_ALL);
  _communicatorThread.start();
}

Communicator::~Communicator() {

  ::curl_global_cleanup();
}

Ticket Communicator::addRequest(Destination destination,
                                std::unique_ptr<GeneralRequest> request,
                                Callbacks callbacks, Options options) {
  uint64_t id = NEXT_TICKET_ID.fetch_add(1, std::memory_order_seq_cst);
  TRI_ASSERT(request);
  GeneralRequest* requestPtr = request.release();
  
  NewRequest* newRequest = new NewRequest(destination, requestPtr, callbacks, options, id);
  //std::unique_ptr<NewRequest> newRequest({destination, std::move(request), callbacks, options, id});
  _ioService->post(std::bind(&CommunicatorThread::createRequest, &_communicatorThread, newRequest));
  
  return Ticket{id};
}


// void Communicator::abortRequest(Ticket ticketId) {
//   //MUTEX_LOCKER(guard, _handlesLock);

//   abortRequestInternal(ticketId);
// }

// void Communicator::abortRequests() {
//   //MUTEX_LOCKER(guard, _handlesLock);

//   for (auto& request : requestsInProgress()) {
//     abortRequestInternal(request->_ticketId);
//   }
// }

// // needs _handlesLock! 
// std::vector<RequestInProgress const*> Communicator::requestsInProgress() {
//   //_handlesLock.assertLockedByCurrentThread();

//   std::vector<RequestInProgress const*> vec;
    
//   vec.reserve(_handlesInProgress.size());

//   for (auto& handle : _handlesInProgress) {
//     RequestInProgress* rip = nullptr;
//     curl_easy_getinfo(handle.second->_handle, CURLINFO_PRIVATE, &rip);
//     TRI_ASSERT(rip != nullptr);
//     vec.push_back(rip);
//   }
//   return vec;
// }

// // needs _handlesLock! 
// void Communicator::abortRequestInternal(Ticket ticketId) {
//   //_handlesLock.assertLockedByCurrentThread();

//   auto handle = _handlesInProgress.find(ticketId);
//   if (handle == _handlesInProgress.end()) {
//     return;
//   }

//   std::string prefix("Communicator(" + std::to_string(handle->second->_rip->_ticketId) +
//                      ") // ");
//   LOG_TOPIC(WARN, Logger::REQUESTS) << prefix << "aborting request to " << handle->second->_rip->_destination.url();
//   handle->second->_rip->_aborted = true;
// }