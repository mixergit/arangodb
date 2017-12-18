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

#include "SimpleHttpClient/CommunicatorThread.h"

#include "Basics/MutexLocker.h"
#include "Rest/HttpRequest.h"
#include "Rest/HttpResponse.h"

using namespace arangodb::communicator;

void CommunicatorThread::createRequest(Ticket const& id, Destination const& destination,
                                GeneralRequest* requestPtr,
                                Callbacks const& callbacks, Options const& options) {
  LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "Adding request to " << destination.url();
  auto generalRequest = std::unique_ptr<GeneralRequest>(requestPtr);
  HttpRequest* request = (HttpRequest*) generalRequest.get();
  TRI_ASSERT(request != nullptr);

  // the curl handle will be managed safely via unique_ptr and hold
  // ownership for rip
  auto rip = new RequestInProgress(
      destination, callbacks, id,
      std::string(request->body().c_str(), request->body().length()),
      options);

  auto handleInProgress = std::make_unique<CurlHandle>(_prototypeHandle, rip);

  CURL* handle = handleInProgress->_handle;
  struct curl_slist* requestHeaders = nullptr;

  switch (request->contentType()) {
    case ContentType::UNSET:
    case ContentType::CUSTOM:
    case ContentType::VPACK:
    case ContentType::DUMP:
      break;
    case ContentType::JSON:
      requestHeaders =
          curl_slist_append(requestHeaders, "Content-Type: application/json");
      break;
    case ContentType::HTML:
      requestHeaders =
          curl_slist_append(requestHeaders, "Content-Type: text/html");
      break;
    case ContentType::TEXT:
      requestHeaders =
          curl_slist_append(requestHeaders, "Content-Type: text/plain");
      break;
  }
  for (auto const& header : request->headers()) {
    std::string thisHeader(header.first + ": " + header.second);
    requestHeaders = curl_slist_append(requestHeaders, thisHeader.c_str());
  }

  std::string url = createSafeDottedCurlUrl(destination.url());
  handleInProgress->_rip->_requestHeaders = requestHeaders;
  curl_easy_setopt(handle, CURLOPT_HTTPHEADER, requestHeaders);
  //curl_easy_setopt(handle, CURLOPT_HEADER, 0L);
  curl_easy_setopt(handle, CURLOPT_URL, url.c_str());

  curl_easy_setopt(handle, CURLOPT_ERRORBUFFER,
                   handleInProgress->_rip.get()->_errorBuffer);

  curl_easy_setopt(handle, CURLOPT_XFERINFODATA, handleInProgress->_rip.get());
  curl_easy_setopt(handle, CURLOPT_WRITEDATA, handleInProgress->_rip.get());
  curl_easy_setopt(handle, CURLOPT_HEADERDATA, handleInProgress->_rip.get());

  long connectTimeout =
      static_cast<long>(options.connectionTimeout);
  // mop: although curl is offering a MS scale connecttimeout this gets ignored
  // in at least 7.50.3
  // in doubt change the timeout to _MS below and hardcode it to 999 and see if
  // the requests immediately fail
  // if not this hack can go away
  if (connectTimeout <= 0) {
    connectTimeout = 1;
  }

  curl_easy_setopt(
      handle, CURLOPT_TIMEOUT_MS,
      static_cast<long>(options.requestTimeout * 1000));
  curl_easy_setopt(handle, CURLOPT_CONNECTTIMEOUT, connectTimeout);

  switch (request->requestType()) {
    // mop: hmmm...why is this stuff in GeneralRequest? we are interested in
    // HTTP only :S
    case RequestType::POST:
      curl_easy_setopt(handle, CURLOPT_POST, 1);
      break;
    case RequestType::PUT:
      // mop: apparently CURLOPT_PUT implies more stuff in curl
      // (for example it adds an expect 100 header)
      // this is not what we want so we make it a custom request
      curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "PUT");
      break;
    case RequestType::DELETE_REQ:
      curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "DELETE");
      break;
    case RequestType::HEAD:
      curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "HEAD");
      break;
    case RequestType::PATCH:
      curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "PATCH");
      break;
    case RequestType::OPTIONS:
      curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "OPTIONS");
      break;
    case RequestType::GET:
      break;
    case RequestType::VSTREAM_CRED:
    case RequestType::VSTREAM_REGISTER:
    case RequestType::VSTREAM_STATUS:
    case RequestType::ILLEGAL:
      throw std::runtime_error(
          "Invalid request type " +
          GeneralRequest::translateMethod(request->requestType()));
      break;
  }

  if (request->body().length() > 0) {
    curl_easy_setopt(handle, CURLOPT_POSTFIELDSIZE,
                     handleInProgress->_rip->_requestBody.length());
    curl_easy_setopt(handle, CURLOPT_POSTFIELDS,
                     handleInProgress->_rip->_requestBody.c_str());
  }

  handleInProgress->_rip->_startTime = TRI_microtime();
 
  { 
    MUTEX_LOCKER(guard, _handlesLock);
    _handlesInProgress.emplace(id, std::move(handleInProgress));
  }
  {
    //MUTEX_LOCKER(guard, _curlLock);
    curl_multi_add_handle(_curl, handle);
  }
}

std::string CommunicatorThread::createSafeDottedCurlUrl(
    std::string const& originalUrl) {
  std::string url;
  url.reserve(originalUrl.length());

  size_t length = originalUrl.length();
  size_t currentFind = 0;
  std::size_t found;
  std::vector<char> urlDotSeparators{'/', '#', '?'};

  while ((found = originalUrl.find("/.", currentFind)) != std::string::npos) {
    if (found + 2 == length) {
      url += originalUrl.substr(currentFind, found - currentFind) + "/%2E";
    } else if (std::find(urlDotSeparators.begin(), urlDotSeparators.end(),
                         originalUrl.at(found + 2)) != urlDotSeparators.end()) {
      url += originalUrl.substr(currentFind, found - currentFind) + "/%2E";
    } else {
      url += originalUrl.substr(currentFind, found - currentFind) + "/.";
    }
    currentFind = found + 2;
  }
  url += originalUrl.substr(currentFind);
  return url;
}

void CommunicatorThread::run() {
  LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "Starting CommunicatorThread";
  _curl = curl_multi_init();
  curl_multi_setopt(_curl, CURLMOPT_SOCKETFUNCTION, &CommunicatorThread::sockCb);
  curl_multi_setopt(_curl, CURLMOPT_SOCKETDATA, this);
  curl_multi_setopt(_curl, CURLMOPT_TIMERFUNCTION, &CommunicatorThread::curlTimerCb);
  curl_multi_setopt(_curl, CURLMOPT_TIMERDATA, this);

  _prototypeHandle = createPrototype();
  _ioService->run();
  curl_easy_cleanup(_prototypeHandle);
}

CURL* CommunicatorThread::createPrototype() {
  CURL* handle = curl_easy_init();
  curl_easy_setopt(handle, CURLOPT_VERBOSE, 0L);
  curl_easy_setopt(handle, CURLOPT_PROXY, "");
  
  // the xfer/progress options are only used to handle request abortions
  curl_easy_setopt(handle, CURLOPT_NOPROGRESS, 0L);
  curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, CommunicatorThread::readBody);

  curl_easy_setopt(handle, CURLOPT_HEADERFUNCTION, CommunicatorThread::readHeaders);

  //curl_easy_setopt(handle, CURLOPT_DEBUGFUNCTION, CommunicatorThread::curlDebug);
  //curl_easy_setopt(handle, CURLOPT_DEBUGDATA, handleInProgress->_rip.get());
    // mop: XXX :S CURLE 51 and 60...
  curl_easy_setopt(handle, CURLOPT_SSL_VERIFYPEER, 0L);
  curl_easy_setopt(handle, CURLOPT_SSL_VERIFYHOST, 0L);

  // boost asio socket stuff
  curl_easy_setopt(handle, CURLOPT_OPENSOCKETFUNCTION, &CommunicatorThread::openSocket);
  curl_easy_setopt(handle, CURLOPT_OPENSOCKETDATA, this);
  curl_easy_setopt(handle, CURLOPT_CLOSESOCKETFUNCTION, &CommunicatorThread::closeSocket);
  curl_easy_setopt(handle, CURLOPT_CLOSESOCKETDATA, this);
  curl_easy_setopt(handle, CURLOPT_XFERINFOFUNCTION, CommunicatorThread::curlProgress);
  curl_easy_setopt(handle, CURLOPT_PATH_AS_IS, 1L); 
  return handle;
}

int CommunicatorThread::sockCb(CURL *e, curl_socket_t s, int what, void *userp, void *sockp) {
  LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "sock_cb: socket=" << s
    << ", what=" << what << ", sockp=" << sockp;

  CommunicatorThread* communicatorThread = (CommunicatorThread*) userp;
  int *actionp = (int *) sockp;
  const char *whatstr[] = { "none", "IN", "OUT", "INOUT", "REMOVE"};

  if(what == CURL_POLL_REMOVE) {
    communicatorThread->removeSocket(actionp);
  } else if(!actionp) {
    LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "Adding data: " << whatstr[what];
    communicatorThread->addSocket(s, e, what);
  } else {
    LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "Changing action from " << whatstr[*actionp] << " to " << whatstr[what];
    communicatorThread->setSocket(actionp, s, e, what, *actionp);
  }

  return 0;
}

/* Update the event timer after curl_multi library calls */
int CommunicatorThread::curlTimerCb(CURLM *multi, long timeout_ms, void* userp) {
  CommunicatorThread* communicatorThread = (CommunicatorThread*) userp;
  LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "multi_timer_cb: " << &communicatorThread->_timer << " timeout_ms " << timeout_ms;
  communicatorThread->_timer.cancel();

  if (timeout_ms > 0) {
    /* update timer */
    communicatorThread->_timer.expires_from_now(boost::posix_time::millisec(timeout_ms));
    communicatorThread->_timer.async_wait(std::bind(&CommunicatorThread::boostTimerCb, communicatorThread, std::placeholders::_1));
  } else if (timeout_ms == 0) {
    /* call timeout function immediately */
    boost::system::error_code error; /*success*/
    communicatorThread->boostTimerCb(error);
  }

  return 0;
}

void CommunicatorThread::addSocket(curl_socket_t s, CURL *easy, int action) {
  /* fdp is used to store current action */ 
  int *fdp = (int *) calloc(sizeof(int), 1);
 
  setSocket(fdp, s, easy, action, 0);
  curl_multi_assign(_curl, s, fdp);
}

void CommunicatorThread::setSocket(int *fdp, curl_socket_t s, CURL *e, int act, int oldact) {
  LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "setsock: socket=" << s << ", act=" << oldact << "=>" << act << ", fdp=" << fdp;

  std::map<curl_socket_t, boost::asio::ip::tcp::socket *>::iterator it =
    _socketMap.find(s);

  if(it == _socketMap.end()) {
    // fprintf(MSG_OUT, "\nsocket %d is a c-ares socket, ignoring", s);
    return;
  }

  boost::asio::ip::tcp::socket * tcp_socket = it->second;

  *fdp = act;

  if(act == CURL_POLL_IN) {
    LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "watching for socket to become readable";
    if(oldact != CURL_POLL_IN && oldact != CURL_POLL_INOUT) {
      tcp_socket->async_read_some(boost::asio::null_buffers(),
                                  std::bind(&CommunicatorThread::eventCb, this, s,
                                              CURL_POLL_IN, std::placeholders::_1, fdp));
    }
  }
  else if(act == CURL_POLL_OUT) {
    LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "watching for socket to become writable";
    if(oldact != CURL_POLL_OUT && oldact != CURL_POLL_INOUT) {
      tcp_socket->async_write_some(boost::asio::null_buffers(),
                                   std::bind(&CommunicatorThread::eventCb, this, s,
                                               CURL_POLL_OUT, std::placeholders::_1, fdp));
    }
  }
  else if(act == CURL_POLL_INOUT) {
    LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "watching for socket to become r/w";
    if(oldact != CURL_POLL_IN && oldact != CURL_POLL_INOUT) {
      tcp_socket->async_read_some(boost::asio::null_buffers(),
                                  std::bind(&CommunicatorThread::eventCb, this, s,
                                              CURL_POLL_IN, std::placeholders::_1, fdp));
    }
    if(oldact != CURL_POLL_OUT && oldact != CURL_POLL_INOUT) {
      tcp_socket->async_write_some(boost::asio::null_buffers(),
                                   std::bind(&CommunicatorThread::eventCb, this, s,
                                               CURL_POLL_OUT, std::placeholders::_1, fdp));
    }
  }
}

void CommunicatorThread::removeSocket(int *f) {
  LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "removeSocket " << f;
 
  if(f) {
    free(f);
  }
}

curl_socket_t CommunicatorThread::openSocket(void *userp, curlsocktype purpose, struct curl_sockaddr *address) {
  CommunicatorThread* communicatorThread = (CommunicatorThread*) userp;
  LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "opensocket";
 
  curl_socket_t sockfd = CURL_SOCKET_BAD;

  /* restrict to IPv4 */
  if(purpose == CURLSOCKTYPE_IPCXN && address->family == AF_INET) {
    /* create a tcp socket object */
    boost::asio::ip::tcp::socket *tcp_socket =
      new boost::asio::ip::tcp::socket(*communicatorThread->_ioService);

    /* open it and get the native handle*/
    boost::system::error_code ec;
    tcp_socket->open(boost::asio::ip::tcp::v4(), ec);

    if(ec) {
      /* An error occurred */
      LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "Couldn't open socket [" << ec << "][" <<
        ec.message() << "]";
    }
    else {
      sockfd = tcp_socket->native_handle();
      // fprintf(MSG_OUT, "\nOpened socket %d", sockfd);

      /* save it for monitoring */
      communicatorThread->_socketMap.insert(std::pair<curl_socket_t,
        boost::asio::ip::tcp::socket *>(sockfd, tcp_socket));
    }
  }

  return sockfd;
}

int CommunicatorThread::closeSocket(void *userp, curl_socket_t item) {
  CommunicatorThread* communicatorThread = (CommunicatorThread*) userp;
  LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "close_socket : " << item;
  
  std::map<curl_socket_t, boost::asio::ip::tcp::socket *>::iterator it =
    communicatorThread->_socketMap.find(item);

  if(it != communicatorThread->_socketMap.end()) {
    delete it->second;
    communicatorThread->_socketMap.erase(it);
  }

  return 0;
}

size_t CommunicatorThread::readBody(void* data, size_t size, size_t nitems,
                              void* userp) {
  RequestInProgress* rip = (struct RequestInProgress*)userp;
  if (rip->_aborted) {
    return 0;
  }
  size_t realsize = size * nitems;
  try {
    rip->_responseBody->appendText((char*)data, realsize);
    return realsize;
  } catch (std::bad_alloc&) {
    return 0;
  }
}

size_t CommunicatorThread::readHeaders(char* buffer, size_t size, size_t nitems,
                                       void* userptr) {
  size_t realsize = size * nitems;
  RequestInProgress* rip = (struct RequestInProgress*)userptr;
  if (rip->_aborted) {
    return 0;
  }

  std::string const header(buffer, realsize);
  size_t pivot = header.find_first_of(':');
  if (pivot != std::string::npos) {
    // mop: hmm response needs lowercased headers
    std::string headerKey =
        basics::StringUtils::tolower(std::string(header.c_str(), pivot));
    rip->_responseHeaders.emplace(
        headerKey, header.substr(pivot + 2, header.length() - pivot - 4));
  }
  return realsize;
}

void CommunicatorThread::logHttpBody(std::string const& prefix,
                               std::string const& data) {
  std::string::size_type n = 0;
  while (n < data.length()) {
    LOG_TOPIC(DEBUG, Logger::COMMUNICATION) << prefix << " "
                                            << data.substr(n, 80);
    n += 80;
  }
}

void CommunicatorThread::logHttpHeaders(std::string const& prefix,
                                  std::string const& headerData) {
  std::string::size_type last = 0;
  std::string::size_type n;
  while (true) {
    n = headerData.find("\r\n", last);
    if (n == std::string::npos) {
      break;
    }
    LOG_TOPIC(DEBUG, Logger::COMMUNICATION)
        << prefix << " " << headerData.substr(last, n - last);
    last = n + 2;
  }
}

int CommunicatorThread::curlDebug(CURL* handle, curl_infotype type, char* data,
                            size_t size, void* userptr) {
  arangodb::communicator::RequestInProgress* request = nullptr;
  curl_easy_getinfo(handle, CURLINFO_PRIVATE, &request);
  TRI_ASSERT(request != nullptr);
  TRI_ASSERT(data != nullptr);

  std::string dataStr(data, size);
  std::string prefix("Communicator(" + std::to_string(request->_ticketId) +
                     ") // ");

  switch (type) {
    case CURLINFO_TEXT:
      LOG_TOPIC(TRACE, Logger::COMMUNICATION) << prefix << "Text: " << dataStr;
      break;
    case CURLINFO_HEADER_OUT:
      logHttpHeaders(prefix + "Header >>", dataStr);
      break;
    case CURLINFO_HEADER_IN:
      logHttpHeaders(prefix + "Header <<", dataStr);
      break;
    case CURLINFO_DATA_OUT:
    case CURLINFO_SSL_DATA_OUT:
      logHttpBody(prefix + "Body >>", dataStr);
      break;
    case CURLINFO_DATA_IN:
    case CURLINFO_SSL_DATA_IN:
      logHttpBody(prefix + "Body <<", dataStr);
      break;
    case CURLINFO_END:
      break;
  }
  return 0;
}

int CommunicatorThread::curlProgress(void* userptr, curl_off_t dltotal,
                               curl_off_t dlnow, curl_off_t ultotal,
                               curl_off_t ulnow) {
  RequestInProgress* rip = (struct RequestInProgress*)userptr;
  return (int) rip->_aborted;
}
 
void CommunicatorThread::boostTimerCb(boost::system::error_code const& error) {
  LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "boost timer cb " << error.message();
  if(!error) {
    handleMultiSocket(CURL_SOCKET_TIMEOUT, 0);
  }
}

/* Called by asio when there is an action on a socket */
void CommunicatorThread::eventCb(curl_socket_t s,
                     int action, const boost::system::error_code & error,
                     int *fdp) {
  LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "event_cb: action=" << action << " error=" << error;
  if(_socketMap.find(s) == _socketMap.end()) {
    LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "event_cb: socket already closed";
    return;
  }

  /* make sure the event matches what are wanted */
  if(*fdp == action || *fdp == CURL_POLL_INOUT) {
    if(error)
      action = CURL_CSELECT_ERR;

    auto result = handleMultiSocket(s, action);
    if (result <= 0) {
      LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "work done. canceling timer " << &_timer;
      _timer.cancel();
      return;
    }

    /* keep on watching.
     * the socket may have been closed and/or fdp may have been changed
     * in curl_multi_socket_action(), so check them both */

      if(!error && _socketMap.find(s) != _socketMap.end() &&
        (*fdp == action || *fdp == CURL_POLL_INOUT)) {
        boost::asio::ip::tcp::socket *tcp_socket = _socketMap.find(s)->second;

        if(action == CURL_POLL_IN) {
          tcp_socket->async_read_some(boost::asio::null_buffers(),
                                      std::bind(&CommunicatorThread::eventCb, this, s,
                                                  action, std::placeholders::_1, fdp));
        }
        if(action == CURL_POLL_OUT) {
          tcp_socket->async_write_some(boost::asio::null_buffers(),
                                      std::bind(&CommunicatorThread::eventCb, this, s,
                                                  action, std::placeholders::_1, fdp));
        }
    }
  }
}

int CommunicatorThread::handleMultiSocket(curl_socket_t s, int const& action) {
  LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "multi socket " << s << " " << action;
  
  int stillRunning = 0;
  CURLMcode rc = curl_multi_socket_action(_curl, s, action, &stillRunning);
  if (rc != CURLM_OK) {
    throw std::runtime_error(
        "Invalid curl multi result while performing! Result was " +
        std::to_string(rc));
  }

  // handle all messages received
  CURLMsg* msg = nullptr;
  int msgsLeft = 0;
  while ((msg = curl_multi_info_read(_curl, &msgsLeft))) {
    if (msg->msg == CURLMSG_DONE) {
      CURL* handle = msg->easy_handle;

      processResult(handle, msg->data.result);
    }
  }
  return stillRunning;
}

void CommunicatorThread::processResult(CURL* handle, CURLcode rc) {
  // remove request in progress
  RequestInProgress* rip = nullptr;
  curl_easy_getinfo(handle, CURLINFO_PRIVATE, &rip);
  if (rip == nullptr) {
    curl_multi_remove_handle(_curl, handle);
    return callResultCallback(
      TRI_ERROR_INTERNAL,
      0,
      Destination(""),
      Callbacks(),
      {nullptr}
    );
  }

  if (rip->_options._curlRcFn) {
    (*rip->_options._curlRcFn)(rc);
  }
  std::string prefix("Communicator(" + std::to_string(rip->_ticketId) +
                     ") // ");
  LOG_TOPIC(TRACE, Logger::COMMUNICATION)
      << prefix << "Curl rc is : " << rc << " after "
      << Logger::FIXED(TRI_microtime() - rip->_startTime) << " s";
  if (strlen(rip->_errorBuffer) != 0) {
    LOG_TOPIC(TRACE, Logger::COMMUNICATION)
        << prefix << "Curl error details: " << rip->_errorBuffer;
  }

  int errorNumber = 0;
  std::unique_ptr<HttpResponse> response;

  //MUTEX_LOCKER(guard, _handlesLock);
  switch (rc) {
    case CURLE_OK: {
      long httpStatusCode = 200;
      curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &httpStatusCode);

      response.reset(new HttpResponse(static_cast<ResponseCode>(httpStatusCode)));
      response->body().swap(rip->_responseBody.get());
      response->setHeaders(std::move(rip->_responseHeaders));

      if (httpStatusCode >= 400) {
        errorNumber = httpStatusCode;
      } // else success leave errorCode 0
      break;
    }
    case CURLE_COULDNT_CONNECT:
    case CURLE_SSL_CONNECT_ERROR:
    case CURLE_COULDNT_RESOLVE_HOST:
    case CURLE_URL_MALFORMAT:
    case CURLE_SEND_ERROR:
      errorNumber = TRI_SIMPLE_CLIENT_COULD_NOT_CONNECT;
      break;
    case CURLE_OPERATION_TIMEDOUT:
    case CURLE_RECV_ERROR:
    case CURLE_GOT_NOTHING:
      if (rip->_aborted) {
        errorNumber = TRI_COMMUNICATOR_REQUEST_ABORTED;
      } else {
        errorNumber= TRI_ERROR_CLUSTER_TIMEOUT;
      }
      break;
    case CURLE_WRITE_ERROR:
      if (rip->_aborted) {
        errorNumber = TRI_COMMUNICATOR_REQUEST_ABORTED;
      } else {
        LOG_TOPIC(ERR, arangodb::Logger::COMMUNICATION) << "Got a write error from curl but request was not aborted";
        errorNumber = TRI_ERROR_INTERNAL;
      }
      break;
    case CURLE_ABORTED_BY_CALLBACK:
      TRI_ASSERT(rip->_aborted);
      errorNumber = TRI_COMMUNICATOR_REQUEST_ABORTED;
      break;
    default:
      LOG_TOPIC(ERR, arangodb::Logger::COMMUNICATION) << "Curl return " << rc;
      errorNumber = TRI_ERROR_INTERNAL;
      break;
  }

  Ticket ticket = rip->_ticketId;
  Destination destination = rip->_destination;
  Callbacks callbacks = rip->_callbacks;
  
  curl_multi_remove_handle(_curl, handle);
  _handlesInProgress.erase(rip->_ticketId);

  callResultCallback(
    errorNumber,
    ticket,
    destination,
    callbacks,
    std::move(response));
}

void CommunicatorThread::callResultCallback(int const& errorNumber,
  Ticket const& ticket,
  Destination const& destination,
  Callbacks const& callbacks,
  std::unique_ptr<HttpResponse> response
) {
  auto start = TRI_microtime();
  if (errorNumber != TRI_ERROR_NO_ERROR) {
    callbacks._onError(errorNumber, std::move(response));
  } else {
    callbacks._onSuccess(std::move(response));
  }
  auto total = TRI_microtime() - start;
  // callbacks are executed from the curl loop..if they take a long time this blocks all traffic!
  // implement an async solution in that case!
  if (total > CALLBACK_WARN_TIME) {
    std::string prefix("Communicator(" + std::to_string(ticket) + ") // ");
    std::string type;
    if (errorNumber != TRI_ERROR_NO_ERROR) {
      type = "error";
    } else {
      type = "success";
    }
    LOG_TOPIC(WARN, Logger::COMMUNICATION) << prefix << type << " callback for request to "
      << destination.url() << " took " << total << "s";
  }
}