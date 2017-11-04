////////////////////////////////////////////////////////////////////////////////
/// @brief test case for EngineInfoContainerCoordinator
///
/// @file
///
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Michael Hackstein
/// @author Copyright 2017, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "catch.hpp"
#include "fakeit.hpp"

#include "Aql/EngineInfoContainerCoordinator.h"
#include "Aql/ExecutionEngine.h"
#include "Aql/ExecutionNode.h"
#include "Aql/Query.h"
#include "Aql/QueryRegistry.h"

using namespace arangodb;
using namespace arangodb::aql;
using namespace fakeit;

namespace arangodb {
namespace tests {
namespace engine_info_container_coordinator_test {

TEST_CASE("EngineInfoContainerCoordinator", "[aql][cluster][coordinator]") {

  SECTION("it should always start with an open snippet, with queryID 0") {
    EngineInfoContainerCoordinator testee;
    QueryId res = testee.closeSnippet();
    REQUIRE(res == 0);
  }

  SECTION("it should be able to add more snippets, all giving a different id") {
    EngineInfoContainerCoordinator testee;

    size_t remote = 1;
    testee.openSnippet(remote);
    testee.openSnippet(remote);

    QueryId res1 = testee.closeSnippet();
    REQUIRE(res1 != 0);

    QueryId res2 = testee.closeSnippet();
    REQUIRE(res2 != res1);
    REQUIRE(res2 != 0);

    QueryId res3 = testee.closeSnippet();
    REQUIRE(res3 == 0);
  }

  ///////////////////////////////////////////
  // SECTION buildEngines
  ///////////////////////////////////////////

  // Flow:
  // 1. Clone the query for every snippet but the first
  // 2. For every snippet:
  //   1. create new Engine (e)
  //   2. query->setEngine(e)
  //   3. query->engine() -> e
  //   4. engine->setLockedShards()
  //   5. engine->createBlocks()
  //   6. Assert (engine->root() != nullptr)
  //   7. For all but the first:
  //     1. queryRegistry->insert(_id, query, 600.0);
  //     2. queryIds.emplace(idOfRemote/dbname, _id);
  // 3. query->engine();

  SECTION("it should create an ExecutionEngine for the first snippet") {

    std::unordered_set<std::string> const restrictToShards;
    std::unordered_map<std::string, std::string> queryIds;
    auto lockedShards = std::make_unique<std::unordered_set<ShardID> const>();
    std::string const dbname = "TestDB";

    // ------------------------------
    // Section: Create Mock Instances
    // ------------------------------
    Mock<ExecutionNode> singletonMock;
    ExecutionNode& sNode = singletonMock.get();
    When(Method(singletonMock, getType)).AlwaysReturn(ExecutionNode::SINGLETON);

    Mock<ExecutionEngine> mockEngine;
    ExecutionEngine& myEngine = mockEngine.get();

    Mock<ExecutionBlock> rootBlockMock;
    ExecutionBlock& rootBlock = rootBlockMock.get();

    Mock<Query> mockQuery;
    Query& query = mockQuery.get();

    Mock<QueryRegistry> mockRegistry;
    QueryRegistry& registry = mockRegistry.get();

    // ------------------------------
    // Section: Mock Functions
    // ------------------------------


    When(Method(mockQuery, setEngine)).Do([&](ExecutionEngine* eng) -> void {
      // We expect that the snippet injects a new engine into our
      // query.
      // However we have to return a mocked engine later
      REQUIRE(eng != nullptr);
      // Throw it away
      delete eng;
    });

    When(Method(mockQuery, engine)).Return(&myEngine).Return(&myEngine);
    When(Method(mockEngine, setLockedShards)).Return();
    When(Method(mockEngine, createBlocks)).Return();
    When(ConstOverloadedMethod(mockEngine, root, ExecutionBlock* ()))
        .AlwaysReturn(&rootBlock);

    // ------------------------------
    // Section: Run the test
    // ------------------------------

    EngineInfoContainerCoordinator testee;
    testee.addNode(&sNode);

    ExecutionEngine* engine = testee.buildEngines(
      &query, &registry, dbname, restrictToShards, queryIds, lockedShards.get() 
    );

    REQUIRE(engine != nullptr);
    REQUIRE(engine == &myEngine);

    // The last engine should not be stored
    // It is not added to the registry
    REQUIRE(queryIds.empty());

    // Validate that the query is wired up with the engine
    Verify(Method(mockQuery, setEngine)).Exactly(1);
    // Validate that lockedShards and createBlocks have been called!
    Verify(Method(mockEngine, setLockedShards)).Exactly(1);
    Verify(Method(mockEngine, createBlocks)).Exactly(1);
  }

  SECTION("it should create an new engine and register it for second snippet") {
    std::unordered_set<std::string> const restrictToShards;
    std::unordered_map<std::string, std::string> queryIds;
    auto lockedShards = std::make_unique<std::unordered_set<ShardID> const>();

    size_t remoteId = 1337;
    QueryId secondId = 0;
    std::string dbname = "TestDB";

    // ------------------------------
    // Section: Create Mock Instances
    // ------------------------------
    Mock<ExecutionNode> firstNodeMock;
    ExecutionNode& fNode = firstNodeMock.get();
    When(Method(firstNodeMock, getType)).AlwaysReturn(ExecutionNode::SINGLETON);

    Mock<ExecutionNode> secondNodeMock;
    ExecutionNode& sNode = secondNodeMock.get();
    When(Method(secondNodeMock, getType)).AlwaysReturn(ExecutionNode::SINGLETON);

    // We need a block only for assertion
    Mock<ExecutionBlock> blockMock;
    ExecutionBlock& block = blockMock.get();

    // Mock engine for first snippet
    Mock<ExecutionEngine> mockEngine;
    ExecutionEngine& myEngine = mockEngine.get();

    // Mock engine for second snippet
    Mock<ExecutionEngine> mockSecondEngine;
    ExecutionEngine& mySecondEngine = mockSecondEngine.get();

    Mock<Query> mockQuery;
    Query& query = mockQuery.get();

    Mock<Query> mockQueryClone;
    Query& queryClone = mockQueryClone.get();

    Mock<QueryRegistry> mockRegistry;
    QueryRegistry& registry = mockRegistry.get();

    // ------------------------------
    // Section: Mock Functions
    // ------------------------------


    When(Method(mockQuery, setEngine)).Do([&](ExecutionEngine* eng) -> void {

      // We expect that the snippet injects a new engine into our
      // query.
      // However we have to return a mocked engine later
      REQUIRE(eng != nullptr);
      // Throw it away
      delete eng;
    });
    When(Method(mockQuery, engine)).Return(&myEngine).Return(&myEngine);
    When(Method(mockEngine, setLockedShards)).Return();
    When(Method(mockEngine, createBlocks)).Do([&](
      std::vector<ExecutionNode*> const& nodes,
      std::unordered_set<std::string> const&,
      std::unordered_set<std::string> const&,
      std::unordered_map<std::string, std::string> const&) {
        REQUIRE(nodes.size() == 1);
        REQUIRE(nodes[0] == &fNode);
    });
    When(ConstOverloadedMethod(mockEngine, root, ExecutionBlock* ()))
        .AlwaysReturn(&block);

    // Mock query clone
    When(Method(mockQuery, clone)).Do([&](QueryPart part, bool withPlan) -> Query* {
      REQUIRE(part == PART_DEPENDENT);
      REQUIRE(withPlan == false);
      return &queryClone;
    });

    When(Method(mockQueryClone, setEngine)).Do([&](ExecutionEngine* eng) -> void {
      // We expect that the snippet injects a new engine into our
      // query.
      // However we have to return a mocked engine later
      REQUIRE(eng != nullptr);
      // Throw it away
      delete eng;
    });

    When(Method(mockQueryClone, engine)).Return(&mySecondEngine);
    When(Method(mockSecondEngine, setLockedShards)).Return();
    When(Method(mockSecondEngine, createBlocks)).Do([&](
      std::vector<ExecutionNode*> const& nodes,
      std::unordered_set<std::string> const&,
      std::unordered_set<std::string> const&,
      std::unordered_map<std::string, std::string> const&) {
        REQUIRE(nodes.size() == 1);
        REQUIRE(nodes[0] == &sNode);
    });
    When(ConstOverloadedMethod(mockSecondEngine, root, ExecutionBlock* ()))
        .AlwaysReturn(&block);

    // Mock the Registry
    When(Method(mockRegistry, insert)).Do([&] (QueryId id, Query* query, double timeout) {
      REQUIRE(id != 0);
      REQUIRE(query != nullptr);
      REQUIRE(timeout == 600.0);
      REQUIRE(query == &queryClone);
      secondId = id;
    });


    // ------------------------------
    // Section: Run the test
    // ------------------------------

    EngineInfoContainerCoordinator testee;
    testee.addNode(&fNode);

    // Open the Second Snippet
    testee.openSnippet(remoteId);
    // Inject a node
    testee.addNode(&sNode);
    // Close the second snippet
    testee.closeSnippet();

    ExecutionEngine* engine = testee.buildEngines(
      &query, &registry, dbname, restrictToShards, queryIds, lockedShards.get() 
    );

    REQUIRE(engine != nullptr);
    REQUIRE(engine == &myEngine);

    // The first engine should not be stored
    // It is not added to the registry
    // The second should be
    REQUIRE(!queryIds.empty());
    // The second engine needs a generated id
    REQUIRE(secondId != 0);

    // It is stored in the mapping
    std::string secIdString = arangodb::basics::StringUtils::itoa(secondId);
    std::string remIdString = arangodb::basics::StringUtils::itoa(remoteId) + "/" + dbname;
    REQUIRE(queryIds.find(remIdString) != queryIds.end());
    REQUIRE(queryIds[remIdString] == secIdString);

    // Validate that the query is wired up with the engine
    Verify(Method(mockQuery, setEngine)).Exactly(1);
    // Validate that lockedShards and createBlocks have been called!
    Verify(Method(mockEngine, setLockedShards)).Exactly(1);
    Verify(Method(mockEngine, createBlocks)).Exactly(1);

    // Validate that the second query is wired up with the second engine
    Verify(Method(mockQueryClone, setEngine)).Exactly(1);
    // Validate that lockedShards and createBlocks have been called!
    Verify(Method(mockSecondEngine, setLockedShards)).Exactly(1);
    Verify(Method(mockSecondEngine, createBlocks)).Exactly(1);
    Verify(Method(mockRegistry, insert)).Exactly(1);
  }

  SECTION("snipets are a stack, insert node always into top snippet") {
    std::unordered_set<std::string> const restrictToShards;
    std::unordered_map<std::string, std::string> queryIds;
    auto lockedShards = std::make_unique<std::unordered_set<ShardID> const>();

    size_t remoteId = 1337;
    size_t secondRemoteId = 42;
    QueryId secondId = 0;
    QueryId thirdId = 0;
    std::string dbname = "TestDB";

    auto setEngineCallback = [] (ExecutionEngine* eng) -> void {
      // We expect that the snippet injects a new engine into our
      // query.
      // However we have to return a mocked engine later
      REQUIRE(eng != nullptr);
      // Throw it away
      delete eng;
    };

    // We test the following:
    // Base Snippet insert node
    // New Snippet (A)
    // Insert Node -> (A)
    // Close (A)
    // Insert Node -> Base
    // New Snippet (B)
    // Insert Node -> (B)
    // Close (B)
    // Insert Node -> Base
    // Verfiy on Engines

    // ------------------------------
    // Section: Create Mock Instances
    // ------------------------------

    Mock<ExecutionNode> firstBaseNodeMock;
    ExecutionNode& fbNode = firstBaseNodeMock.get();
    When(Method(firstBaseNodeMock, getType)).AlwaysReturn(ExecutionNode::SINGLETON);

    Mock<ExecutionNode> snipANodeMock;
    ExecutionNode& aNode = snipANodeMock.get();
    When(Method(snipANodeMock, getType)).AlwaysReturn(ExecutionNode::SINGLETON);

    Mock<ExecutionNode> secondBaseNodeMock;
    ExecutionNode& sbNode = secondBaseNodeMock.get();
    When(Method(secondBaseNodeMock, getType)).AlwaysReturn(ExecutionNode::SINGLETON);

    Mock<ExecutionNode> snipBNodeMock;
    ExecutionNode& bNode = snipBNodeMock.get();
    When(Method(snipBNodeMock, getType)).AlwaysReturn(ExecutionNode::SINGLETON);

    Mock<ExecutionNode> thirdBaseNodeMock;
    ExecutionNode& tbNode = thirdBaseNodeMock.get();
    When(Method(thirdBaseNodeMock, getType)).AlwaysReturn(ExecutionNode::SINGLETON);

    // We need a block only for assertion
    Mock<ExecutionBlock> blockMock;
    ExecutionBlock& block = blockMock.get();

    // Mock engine for first snippet
    Mock<ExecutionEngine> mockEngine;
    ExecutionEngine& myEngine = mockEngine.get();

    // Mock engine for second snippet
    Mock<ExecutionEngine> mockSecondEngine;
    ExecutionEngine& mySecondEngine = mockSecondEngine.get();

    // Mock engine for second snippet
    Mock<ExecutionEngine> mockThirdEngine;
    ExecutionEngine& myThirdEngine = mockThirdEngine.get();

    Mock<Query> mockQuery;
    Query& query = mockQuery.get();

    // We need two query clones
    Mock<Query> mockQueryClone;
    Query& queryClone = mockQueryClone.get();

    Mock<Query> mockQuerySecondClone;
    Query& querySecondClone = mockQuerySecondClone.get();

    Mock<QueryRegistry> mockRegistry;
    QueryRegistry& registry = mockRegistry.get();

    // ------------------------------
    // Section: Mock Functions
    // ------------------------------

    When(Method(mockQuery, setEngine)).Do(setEngineCallback);
    When(Method(mockQuery, engine)).Return(&myEngine).Return(&myEngine);
    When(Method(mockEngine, setLockedShards)).Return();
    When(Method(mockEngine, createBlocks)).Do([&](
      std::vector<ExecutionNode*> const& nodes,
      std::unordered_set<std::string> const&,
      std::unordered_set<std::string> const&,
      std::unordered_map<std::string, std::string> const&) {
        REQUIRE(nodes.size() == 3);
        REQUIRE(nodes[0] == &fbNode);
        REQUIRE(nodes[1] == &sbNode);
        REQUIRE(nodes[2] == &tbNode);
    });
    When(ConstOverloadedMethod(mockEngine, root, ExecutionBlock* ()))
        .AlwaysReturn(&block);

    When(Method(mockQuery, clone)).Do([&](QueryPart part, bool withPlan) -> Query* {
      REQUIRE(part == PART_DEPENDENT);
      REQUIRE(withPlan == false);
      return &queryClone;
    }).Do([&](QueryPart part, bool withPlan) -> Query* {
      REQUIRE(part == PART_DEPENDENT);
      REQUIRE(withPlan == false);
      return &querySecondClone;
    });


    // Mock first clone
    When(Method(mockQueryClone, setEngine)).Do(setEngineCallback);
    When(Method(mockQueryClone, engine)).Return(&mySecondEngine);
    When(Method(mockSecondEngine, setLockedShards)).Return();
    When(Method(mockSecondEngine, createBlocks)).Do([&](
      std::vector<ExecutionNode*> const& nodes,
      std::unordered_set<std::string> const&,
      std::unordered_set<std::string> const&,
      std::unordered_map<std::string, std::string> const&) {
        REQUIRE(nodes.size() == 1);
        REQUIRE(nodes[0] == &aNode);
    });
    When(ConstOverloadedMethod(mockSecondEngine, root, ExecutionBlock* ()))
        .AlwaysReturn(&block);

    // Mock second clone
    When(Method(mockQuerySecondClone, setEngine)).Do(setEngineCallback);
    When(Method(mockQuerySecondClone, engine)).Return(&myThirdEngine);
    When(Method(mockThirdEngine, setLockedShards)).Return();
    When(Method(mockThirdEngine, createBlocks)).Do([&](
      std::vector<ExecutionNode*> const& nodes,
      std::unordered_set<std::string> const&,
      std::unordered_set<std::string> const&,
      std::unordered_map<std::string, std::string> const&) {
        REQUIRE(nodes.size() == 1);
        REQUIRE(nodes[0] == &bNode);
    });
    When(ConstOverloadedMethod(mockThirdEngine, root, ExecutionBlock* ()))
        .AlwaysReturn(&block);

    // Mock the Registry
    // NOTE: This expects an ordering of the engines first of the stack will be handled
    // first. With same fakeit magic we could make this ordering independent which is
    // is fine as well for the production code.
    When(Method(mockRegistry, insert)).Do([&] (QueryId id, Query* query, double timeout) {
      REQUIRE(id != 0);
      REQUIRE(query != nullptr);
      REQUIRE(timeout == 600.0);
      REQUIRE(query == &queryClone);
      secondId = id;
    }).Do([&] (QueryId id, Query* query, double timeout) {
      REQUIRE(id != 0);
      REQUIRE(query != nullptr);
      REQUIRE(timeout == 600.0);
      REQUIRE(query == &querySecondClone);
      thirdId = id;
    });


    // ------------------------------
    // Section: Run the test
    // ------------------------------
    EngineInfoContainerCoordinator testee;

    testee.addNode(&fbNode);

    testee.openSnippet(remoteId);
    testee.addNode(&aNode);
    testee.closeSnippet();

    testee.addNode(&sbNode);

    testee.openSnippet(secondRemoteId);
    testee.addNode(&bNode);
    testee.closeSnippet();

    testee.addNode(&tbNode);

    ExecutionEngine* engine = testee.buildEngines(
      &query, &registry, dbname, restrictToShards, queryIds, lockedShards.get() 
    );

    REQUIRE(engine != nullptr);
    REQUIRE(engine == &myEngine);

    // The first engine should not be stored
    // It is not added to the registry
    // The other two should be
    REQUIRE(queryIds.size() == 2);

    // First (A) is stored in the mapping
    std::string secIdString = arangodb::basics::StringUtils::itoa(secondId);
    std::string remIdString = arangodb::basics::StringUtils::itoa(remoteId) + "/" + dbname;
    REQUIRE(queryIds.find(remIdString) != queryIds.end());
    REQUIRE(queryIds[remIdString] == secIdString);

    // Second (B) is stored in the mapping
    std::string thirdIdString = arangodb::basics::StringUtils::itoa(thirdId);
    std::string secRemIdString = arangodb::basics::StringUtils::itoa(secondRemoteId) + "/" + dbname;
    REQUIRE(queryIds.find(secRemIdString) != queryIds.end());
    REQUIRE(queryIds[secRemIdString] == thirdIdString);

    // Validate that the query is wired up with the engine
    Verify(Method(mockQuery, setEngine)).Exactly(1);
    // Validate that lockedShards and createBlocks have been called!
    Verify(Method(mockEngine, setLockedShards)).Exactly(1);
    Verify(Method(mockEngine, createBlocks)).Exactly(1);

    // Validate that the second query is wired up with the second engine
    Verify(Method(mockQueryClone, setEngine)).Exactly(1);
    // Validate that lockedShards and createBlocks have been called!
    Verify(Method(mockSecondEngine, setLockedShards)).Exactly(1);
    Verify(Method(mockSecondEngine, createBlocks)).Exactly(1);

    // Validate that the second query is wired up with the second engine
    Verify(Method(mockQuerySecondClone, setEngine)).Exactly(1);
    // Validate that lockedShards and createBlocks have been called!
    Verify(Method(mockThirdEngine, setLockedShards)).Exactly(1);
    Verify(Method(mockThirdEngine, createBlocks)).Exactly(1);

    // Validate two queries are registered correctly
    Verify(Method(mockRegistry, insert)).Exactly(2);
  }

  SECTION("it should unregister all engines on error in query clone") {
    std::unordered_set<std::string> const restrictToShards;
    std::unordered_map<std::string, std::string> queryIds;
    auto lockedShards = std::make_unique<std::unordered_set<ShardID> const>();

    size_t remoteId = 1337;
    QueryId secondId = 0;
    std::string dbname = "TestDB";

    // ------------------------------
    // Section: Create Mock Instances
    // ------------------------------
    Mock<ExecutionNode> firstNodeMock;
    ExecutionNode& fNode = firstNodeMock.get();
    When(Method(firstNodeMock, getType)).AlwaysReturn(ExecutionNode::SINGLETON);

    // We need a block only for assertion
    Mock<ExecutionBlock> blockMock;
    ExecutionBlock& block = blockMock.get();

    // Mock engine for first snippet
    Mock<ExecutionEngine> mockEngine;
    ExecutionEngine& myEngine = mockEngine.get();

    // Mock engine for second snippet
    Mock<ExecutionEngine> mockSecondEngine;
    ExecutionEngine& mySecondEngine = mockSecondEngine.get();

    Mock<Query> mockQuery;
    Query& query = mockQuery.get();

    Mock<Query> mockQueryClone;
    Query& queryClone = mockQueryClone.get();

    Mock<QueryRegistry> mockRegistry;
    QueryRegistry& registry = mockRegistry.get();

    // ------------------------------
    // Section: Mock Functions
    // ------------------------------


    When(Method(mockQuery, setEngine)).Do([&](ExecutionEngine* eng) -> void {

      // We expect that the snippet injects a new engine into our
      // query.
      // However we have to return a mocked engine later
      REQUIRE(eng != nullptr);
      // Throw it away
      delete eng;
    });
    When(Method(mockQuery, engine)).Return(&myEngine).Return(&myEngine);
    When(Method(mockEngine, setLockedShards)).Return();
    When(Method(mockEngine, createBlocks)).AlwaysReturn();
    When(ConstOverloadedMethod(mockEngine, root, ExecutionBlock* ()))
        .AlwaysReturn(&block);

    // Mock query clone
    When(Method(mockQuery, clone)).Do([&](QueryPart part, bool withPlan) -> Query* {
      REQUIRE(part == PART_DEPENDENT);
      REQUIRE(withPlan == false);
      return &queryClone;
    }).Throw(arangodb::basics::Exception(TRI_ERROR_DEBUG, __FILE__, __LINE__));

    When(Method(mockQueryClone, setEngine)).Do([&](ExecutionEngine* eng) -> void {
      // We expect that the snippet injects a new engine into our
      // query.
      // However we have to return a mocked engine later
      REQUIRE(eng != nullptr);
      // Throw it away
      delete eng;
    });

    When(Method(mockQueryClone, engine)).Return(&mySecondEngine);
    When(Method(mockSecondEngine, setLockedShards)).Return();
    When(Method(mockSecondEngine, createBlocks)).AlwaysReturn();
    When(ConstOverloadedMethod(mockSecondEngine, root, ExecutionBlock* ()))
        .AlwaysReturn(&block);

    // Mock the Registry
    When(Method(mockRegistry, insert)).Do([&] (QueryId id, Query* query, double timeout) {
      REQUIRE(id != 0);
      REQUIRE(query != nullptr);
      REQUIRE(timeout == 600.0);
      REQUIRE(query == &queryClone);
      secondId = id;
    });
    When(OverloadedMethod(mockRegistry, destroy, void(std::string const&, QueryId, int))).Do([&]
          (std::string const& vocbase, QueryId id, int errorCode) {
      REQUIRE(vocbase == dbname);
      REQUIRE(id == secondId);
      REQUIRE(errorCode == TRI_ERROR_DEBUG);
    });


    // ------------------------------
    // Section: Run the test
    // ------------------------------

    EngineInfoContainerCoordinator testee;
    testee.addNode(&fNode);

    // Open the Second Snippet
    testee.openSnippet(remoteId);
    // Inject a node
    testee.addNode(&fNode);

    testee.openSnippet(remoteId);
    // Inject a node
    testee.addNode(&fNode);

    // Close the third snippet
    testee.closeSnippet();

    // Close the second snippet
    testee.closeSnippet();


    try {
      ExecutionEngine* engine = testee.buildEngines(
        &query, &registry, dbname, restrictToShards, queryIds, lockedShards.get() 
      );
      // We should never get here, above needs to yield errors
      REQUIRE(true == false);
    } catch (basics::Exception& ex) {
      // Make sure we check the right thing here
      REQUIRE(ex.code() == TRI_ERROR_DEBUG);

      // Validate that the path up to intended error was taken

      // Validate that the query is wired up with the engine
      Verify(Method(mockQuery, setEngine)).Exactly(1);
      // Validate that lockedShards and createBlocks have been called!
      Verify(Method(mockEngine, setLockedShards)).Exactly(1);
      Verify(Method(mockEngine, createBlocks)).Exactly(1);

      // Validate that the second query is wired up with the second engine
      Verify(Method(mockQueryClone, setEngine)).Exactly(1);
      // Validate that lockedShards and createBlocks have been called!
      Verify(Method(mockSecondEngine, setLockedShards)).Exactly(1);
      Verify(Method(mockSecondEngine, createBlocks)).Exactly(1);
      Verify(Method(mockRegistry, insert)).Exactly(1);

      // Assert unregister of second engine.
      Verify(OverloadedMethod(mockRegistry, destroy, void(std::string const&, QueryId, int))).Exactly(1);
    }
  }

}
} // test
} // aql
} // arangodb
