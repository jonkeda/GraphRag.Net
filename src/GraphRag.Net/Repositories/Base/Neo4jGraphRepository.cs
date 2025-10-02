using GraphRag.Net.Options;
using GraphRag.Net.Repositories;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Polly;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace GraphRag.Net.Base
{
    /// <summary>
    /// Neo4j实现的图数据库存储
    /// </summary>
    public class Neo4jGraphRepository : IGraphRepository, IDisposable
    {
        private readonly IDriver _driver;
        private readonly ILogger<Neo4jGraphRepository> _logger;
        private readonly string _database;
        private readonly ResiliencePipeline _retryPipeline;
        private bool _constraintsCreated = false;
        private readonly SemaphoreSlim _constraintSemaphore = new(1, 1);

        public Neo4jGraphRepository(ILogger<Neo4jGraphRepository> logger)
        {
            _logger = logger;
            _database = GraphDBConnectionOption.Neo4jDatabase;
            
            var uri = GraphDBConnectionOption.DBConnection;
            var username = GraphDBConnectionOption.Neo4jUsername;
            var password = GraphDBConnectionOption.Neo4jPassword;

            _driver = GraphDatabase.Driver(uri, AuthTokens.Basic(username, password));

            // Setup retry policy for transient errors
            _retryPipeline = new ResiliencePipelineBuilder()
                .AddRetry(new Polly.Retry.RetryStrategyOptions
                {
                    MaxRetryAttempts = 3,
                    DelayGenerator = args => ValueTask.FromResult<TimeSpan?>(TimeSpan.FromSeconds(Math.Pow(2, args.AttemptNumber))),
                    ShouldHandle = args => ValueTask.FromResult(IsTransientError(args.Outcome.Exception))
                })
                .Build();
        }

        private bool IsTransientError(Exception? ex)
        {
            return ex is Neo4jException neo4jEx &&
                   (neo4jEx.Code?.Contains("Neo.TransientError") == true ||
                    neo4jEx.Code?.Contains("ServiceUnavailable") == true);
        }

        private string GenerateDeterministicEdgeId(string source, string target, string relationship, string index)
        {
            // Normalize direction for deterministic IDs
            var (normalizedSource, normalizedTarget, reversed) = NormalizeDirection(source, target);
            var input = $"{normalizedSource}|{normalizedTarget}|{relationship}|{index}";
            
            using var sha256 = SHA256.Create();
            var hash = sha256.ComputeHash(Encoding.UTF8.GetBytes(input));
            return Convert.ToHexString(hash).ToLowerInvariant();
        }

        private (string source, string target, bool reversed) NormalizeDirection(string source, string target)
        {
            // Ensure consistent direction by lexicographic ordering
            var comparison = string.Compare(source, target, StringComparison.Ordinal);
            if (comparison <= 0)
            {
                return (source, target, false);
            }
            else
            {
                return (target, source, true);
            }
        }

        private async Task EnsureConstraintsAsync()
        {
            if (_constraintsCreated) return;

            await _constraintSemaphore.WaitAsync();
            try
            {
                if (_constraintsCreated) return;

                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                
                // Create unique constraint on Node.id
                await session.ExecuteWriteAsync(async tx =>
                {
                    try
                    {
                        await tx.RunAsync("CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "Node constraint may already exist");
                    }
                    return Task.CompletedTask;
                });

                // Create index on Node.index
                await session.ExecuteWriteAsync(async tx =>
                {
                    try
                    {
                        await tx.RunAsync("CREATE INDEX node_index IF NOT EXISTS FOR (n:Node) ON (n.index)");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "Node index may already exist");
                    }
                    return Task.CompletedTask;
                });

                _constraintsCreated = true;
                _logger.LogInformation("Neo4j constraints and indexes created successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to create Neo4j constraints");
                throw;
            }
            finally
            {
                _constraintSemaphore.Release();
            }
        }

        private async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation)
        {
            return await _retryPipeline.ExecuteAsync(async (cancellationToken) =>
            {
                return await operation();
            });
        }

        // Nodes operations
        public async Task<List<Nodes>> GetNodesAsync(string index)
        {
            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(
                        "MATCH (n:Node {index: $index}) RETURN n.id as id, n.name as name, n.type as type, n.desc as desc",
                        new { index });
                    
                    var nodes = new List<Nodes>();
                    await foreach (var record in cursor)
                    {
                        nodes.Add(new Nodes
                        {
                            Id = record["id"].As<string>(),
                            Index = index,
                            Name = record["name"].As<string>(),
                            Type = record["type"].As<string>(),
                            Desc = record["desc"].As<string>()
                        });
                    }
                    return nodes;
                });
                
                _logger.LogDebug("Retrieved {Count} nodes for index {Index}", result.Count, index);
                return result;
            });
        }

        public async Task<List<Nodes>> GetNodesByIdsAsync(string index, List<string> nodeIds)
        {
            if (!nodeIds.Any()) return new List<Nodes>();

            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(
                        "MATCH (n:Node) WHERE n.index = $index AND n.id IN $nodeIds RETURN n.id as id, n.name as name, n.type as type, n.desc as desc",
                        new { index, nodeIds });
                    
                    var nodes = new List<Nodes>();
                    await foreach (var record in cursor)
                    {
                        nodes.Add(new Nodes
                        {
                            Id = record["id"].As<string>(),
                            Index = index,
                            Name = record["name"].As<string>(),
                            Type = record["type"].As<string>(),
                            Desc = record["desc"].As<string>()
                        });
                    }
                    return nodes;
                });
                
                return result;
            });
        }

        public async Task<bool> InsertNodeAsync(Nodes node)
        {
            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                await session.ExecuteWriteAsync(async tx =>
                {
                    await tx.RunAsync(
                        "MERGE (n:Node {id: $id}) SET n.index = $index, n.name = $name, n.type = $type, n.desc = $desc",
                        new { 
                            id = node.Id, 
                            index = node.Index, 
                            name = node.Name, 
                            type = node.Type, 
                            desc = node.Desc 
                        });
                    return Task.CompletedTask;
                });
                
                _logger.LogDebug("Inserted node {NodeId}", node.Id);
                return true;
            });
        }

        public async Task<bool> InsertNodesAsync(List<Nodes> nodes)
        {
            return await BulkInsertNodesAsync(nodes);
        }

        public async Task<bool> UpdateNodeAsync(Nodes node)
        {
            return await InsertNodeAsync(node); // MERGE handles both insert and update
        }

        public async Task<bool> DeleteNodeAsync(string nodeId)
        {
            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                var result = await session.ExecuteWriteAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(
                        "MATCH (n:Node {id: $id}) DETACH DELETE n RETURN count(n) as deleted",
                        new { id = nodeId });
                    
                    var record = await cursor.SingleAsync();
                    return record["deleted"].As<int>() > 0;
                });
                
                _logger.LogDebug("Deleted node {NodeId}: {Success}", nodeId, result);
                return result;
            });
        }

        public async Task<bool> NodeExistsAsync(string nodeId)
        {
            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(
                        "MATCH (n:Node {id: $id}) RETURN count(n) > 0 as exists",
                        new { id = nodeId });
                    
                    var record = await cursor.SingleAsync();
                    return record["exists"].As<bool>();
                });
                
                return result;
            });
        }

        public async Task<Nodes?> GetNodeByIdAsync(string nodeId)
        {
            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(
                        "MATCH (n:Node {id: $id}) RETURN n.id as id, n.index as index, n.name as name, n.type as type, n.desc as desc",
                        new { id = nodeId });
                    
                    var records = await cursor.ToListAsync();
                    if (!records.Any()) return null;
                    
                    var record = records.First();
                    
                    return new Nodes
                    {
                        Id = record["id"].As<string>(),
                        Index = record["index"].As<string>(),
                        Name = record["name"].As<string>(),
                        Type = record["type"].As<string>(),
                        Desc = record["desc"].As<string>()
                    };
                });
                
                return result;
            });
        }

        // Edges operations
        public async Task<List<Edges>> GetEdgesAsync(string index)
        {
            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(
                        """
                        MATCH (source:Node)-[r:RELATES_TO]->(target:Node) 
                        WHERE r.index = $index 
                        RETURN r.id as id, source.id as source, target.id as target, r.relationship as relationship, r.reversed as reversed
                        """,
                        new { index });
                    
                    var edges = new List<Edges>();
                    await foreach (var record in cursor)
                    {
                        var sourceId = record["source"].As<string>();
                        var targetId = record["target"].As<string>();
                        var reversed = record["reversed"].As<bool>();
                        
                        // Reverse back if needed
                        if (reversed)
                        {
                            (sourceId, targetId) = (targetId, sourceId);
                        }
                        
                        edges.Add(new Edges
                        {
                            Id = record["id"].As<string>(),
                            Index = index,
                            Source = sourceId,
                            Target = targetId,
                            Relationship = record["relationship"].As<string>()
                        });
                    }
                    return edges;
                });
                
                _logger.LogDebug("Retrieved {Count} edges for index {Index}", result.Count, index);
                return result;
            });
        }

        public async Task<List<Edges>> GetEdgesBySourceAsync(string index, string sourceId)
        {
            var allEdges = await GetEdgesAsync(index);
            return allEdges.Where(e => e.Source == sourceId).ToList();
        }

        public async Task<List<Edges>> GetEdgesByTargetAsync(string index, string targetId)
        {
            var allEdges = await GetEdgesAsync(index);
            return allEdges.Where(e => e.Target == targetId).ToList();
        }

        public async Task<List<Edges>> GetEdgesByNodeAsync(string index, string nodeId)
        {
            var allEdges = await GetEdgesAsync(index);
            return allEdges.Where(e => e.Source == nodeId || e.Target == nodeId).ToList();
        }

        public async Task<bool> InsertEdgeAsync(Edges edge)
        {
            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                var deterministicId = GenerateDeterministicEdgeId(edge.Source, edge.Target, edge.Relationship, edge.Index);
                var (normalizedSource, normalizedTarget, reversed) = NormalizeDirection(edge.Source, edge.Target);
                
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                await session.ExecuteWriteAsync(async tx =>
                {
                    // Check for existing relationship with semantic merge
                    var existingCursor = await tx.RunAsync(
                        """
                        MATCH (source:Node {id: $normalizedSource}), (target:Node {id: $normalizedTarget})
                        OPTIONAL MATCH (source)-[r:RELATES_TO]->(target)
                        WHERE r.index = $index
                        RETURN r.relationship as existing_relationship, r.id as existing_id
                        """,
                        new { normalizedSource, normalizedTarget, index = edge.Index });
                    
                    var existingRecords = await existingCursor.ToListAsync();
                    var existingRecord = existingRecords.FirstOrDefault();
                    var existingRelationship = existingRecord?["existing_relationship"]?.As<string>();
                    var existingId = existingRecord?["existing_id"]?.As<string>();
                    
                    string finalRelationship;
                    if (!string.IsNullOrEmpty(existingRelationship))
                    {
                        // Semantic merge: combine relationships
                        finalRelationship = MergeRelationships(existingRelationship, edge.Relationship);
                    }
                    else
                    {
                        finalRelationship = edge.Relationship;
                    }
                    
                    // Create or update relationship
                    await tx.RunAsync(
                        """
                        MATCH (source:Node {id: $normalizedSource}), (target:Node {id: $normalizedTarget})
                        MERGE (source)-[r:RELATES_TO]->(target)
                        SET r.id = $deterministicId, r.index = $index, r.relationship = $relationship, r.reversed = $reversed
                        """,
                        new { 
                            normalizedSource, 
                            normalizedTarget, 
                            deterministicId, 
                            index = edge.Index, 
                            relationship = finalRelationship, 
                            reversed 
                        });
                    
                    return Task.CompletedTask;
                });
                
                _logger.LogDebug("Inserted edge {EdgeId} from {Source} to {Target}", deterministicId, edge.Source, edge.Target);
                return true;
            });
        }

        private string MergeRelationships(string existing, string newRelationship)
        {
            if (existing == newRelationship) return existing;
            
            // Simple semantic merge: combine with semicolon if different
            var existingParts = existing.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim()).ToHashSet();
            var newParts = newRelationship.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim());
            
            foreach (var part in newParts)
            {
                existingParts.Add(part);
            }
            
            return string.Join("; ", existingParts);
        }

        public async Task<bool> InsertEdgesAsync(List<Edges> edges)
        {
            return await BulkInsertEdgesAsync(edges);
        }

        public async Task<bool> UpdateEdgeAsync(Edges edge)
        {
            return await InsertEdgeAsync(edge); // MERGE handles both insert and update
        }

        public async Task<bool> DeleteEdgeAsync(string edgeId)
        {
            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                var result = await session.ExecuteWriteAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(
                        "MATCH ()-[r:RELATES_TO {id: $id}]-() DELETE r RETURN count(r) as deleted",
                        new { id = edgeId });
                    
                    var record = await cursor.SingleAsync();
                    return record["deleted"].As<int>() > 0;
                });
                
                _logger.LogDebug("Deleted edge {EdgeId}: {Success}", edgeId, result);
                return result;
            });
        }

        public async Task<bool> EdgeExistsAsync(string sourceId, string targetId, string index)
        {
            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                var (normalizedSource, normalizedTarget, _) = NormalizeDirection(sourceId, targetId);
                
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(
                        """
                        MATCH (source:Node {id: $normalizedSource})-[r:RELATES_TO]->(target:Node {id: $normalizedTarget})
                        WHERE r.index = $index
                        RETURN count(r) > 0 as exists
                        """,
                        new { normalizedSource, normalizedTarget, index });
                    
                    var record = await cursor.SingleAsync();
                    return record["exists"].As<bool>();
                });
                
                return result;
            });
        }

        public async Task<Edges?> GetEdgeByIdAsync(string edgeId)
        {
            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(
                        """
                        MATCH (source:Node)-[r:RELATES_TO {id: $id}]->(target:Node)
                        RETURN r.id as id, r.index as index, source.id as source, target.id as target, r.relationship as relationship, r.reversed as reversed
                        """,
                        new { id = edgeId });
                    
                    var records = await cursor.ToListAsync();
                    if (!records.Any()) return null;
                    
                    var record = records.First();
                    
                    var sourceId = record["source"].As<string>();
                    var targetId = record["target"].As<string>();
                    var reversed = record["reversed"].As<bool>();
                    
                    // Reverse back if needed
                    if (reversed)
                    {
                        (sourceId, targetId) = (targetId, sourceId);
                    }
                    
                    return new Edges
                    {
                        Id = record["id"].As<string>(),
                        Index = record["index"].As<string>(),
                        Source = sourceId,
                        Target = targetId,
                        Relationship = record["relationship"].As<string>()
                    };
                });
                
                return result;
            });
        }

        // Graph operations
        public async Task<(List<Nodes> nodes, List<Edges> edges)> GetGraphAsync(string index)
        {
            var nodes = await GetNodesAsync(index);
            var edges = await GetEdgesAsync(index);
            return (nodes, edges);
        }

        public async Task<(List<Nodes> nodes, List<Edges> edges)> GetSubGraphAsync(string index, List<string> nodeIds, int maxDepth = 1)
        {
            if (!nodeIds.Any()) return (new List<Nodes>(), new List<Edges>());

            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                var result = await session.ExecuteReadAsync(async tx =>
                {
                    // Get subgraph using Neo4j's path traversal
                    var cursor = await tx.RunAsync(
                        $"""
                        MATCH path = (start:Node)-[*1..{maxDepth}]-(connected:Node)
                        WHERE start.id IN $nodeIds AND start.index = $index AND connected.index = $index
                        WITH DISTINCT nodes(path) as pathNodes, relationships(path) as pathRels
                        UNWIND pathNodes as n
                        WITH DISTINCT n, pathRels
                        UNWIND pathRels as r
                        RETURN DISTINCT 
                            n.id as nodeId, n.index as nodeIndex, n.name as nodeName, n.type as nodeType, n.desc as nodeDesc,
                            r.id as relId, r.index as relIndex, startNode(r).id as relSource, endNode(r).id as relTarget, 
                            r.relationship as relationship, r.reversed as reversed
                        """,
                        new { nodeIds, index });
                    
                    var nodes = new Dictionary<string, Nodes>();
                    var edges = new Dictionary<string, Edges>();
                    
                    await foreach (var record in cursor)
                    {
                        // Add node
                        var nodeId = record["nodeId"].As<string>();
                        if (!nodes.ContainsKey(nodeId))
                        {
                            nodes[nodeId] = new Nodes
                            {
                                Id = nodeId,
                                Index = record["nodeIndex"].As<string>(),
                                Name = record["nodeName"].As<string>(),
                                Type = record["nodeType"].As<string>(),
                                Desc = record["nodeDesc"].As<string>()
                            };
                        }
                        
                        // Add edge
                        var relId = record["relId"].As<string>();
                        if (!string.IsNullOrEmpty(relId) && !edges.ContainsKey(relId))
                        {
                            var sourceId = record["relSource"].As<string>();
                            var targetId = record["relTarget"].As<string>();
                            var reversed = record["reversed"].As<bool>();
                            
                            // Reverse back if needed
                            if (reversed)
                            {
                                (sourceId, targetId) = (targetId, sourceId);
                            }
                            
                            edges[relId] = new Edges
                            {
                                Id = relId,
                                Index = record["relIndex"].As<string>(),
                                Source = sourceId,
                                Target = targetId,
                                Relationship = record["relationship"].As<string>()
                            };
                        }
                    }
                    
                    return (nodes.Values.ToList(), edges.Values.ToList());
                });
                
                _logger.LogDebug("Retrieved subgraph with {NodeCount} nodes and {EdgeCount} edges", result.Item1.Count, result.Item2.Count);
                return result;
            });
        }

        // Batch operations
        public async Task<bool> BulkInsertNodesAsync(List<Nodes> nodes)
        {
            if (!nodes.Any()) return true;

            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                var batchSize = 1000;
                var success = true;
                
                for (var i = 0; i < nodes.Count; i += batchSize)
                {
                    var batch = nodes.Skip(i).Take(batchSize).ToList();
                    
                    await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                    await session.ExecuteWriteAsync(async tx =>
                    {
                        var nodeParams = batch.Select((node, index) => new Dictionary<string, object>
                        {
                            [$"id{index}"] = node.Id,
                            [$"index{index}"] = node.Index,
                            [$"name{index}"] = node.Name,
                            [$"type{index}"] = node.Type,
                            [$"desc{index}"] = node.Desc
                        }).Aggregate((dict1, dict2) => dict1.Concat(dict2).ToDictionary(kv => kv.Key, kv => kv.Value));

                        var query = "UNWIND $nodes AS nodeData " +
                                   "MERGE (n:Node {id: nodeData.id}) " +
                                   "SET n.index = nodeData.index, n.name = nodeData.name, n.type = nodeData.type, n.desc = nodeData.desc";

                        var nodeDataList = batch.Select(node => new
                        {
                            id = node.Id,
                            index = node.Index,
                            name = node.Name,
                            type = node.Type,
                            desc = node.Desc
                        }).ToList();

                        await tx.RunAsync(query, new { nodes = nodeDataList });
                        return Task.CompletedTask;
                    });
                    
                    _logger.LogDebug("Bulk inserted batch of {Count} nodes", batch.Count);
                }
                
                _logger.LogInformation("Bulk inserted {TotalCount} nodes successfully", nodes.Count);
                return success;
            });
        }

        public async Task<bool> BulkInsertEdgesAsync(List<Edges> edges)
        {
            if (!edges.Any()) return true;

            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                var batchSize = 1000;
                var success = true;
                
                for (var i = 0; i < edges.Count; i += batchSize)
                {
                    var batch = edges.Skip(i).Take(batchSize).ToList();
                    
                    await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                    await session.ExecuteWriteAsync(async tx =>
                    {
                        var edgeDataList = batch.Select(edge =>
                        {
                            var deterministicId = GenerateDeterministicEdgeId(edge.Source, edge.Target, edge.Relationship, edge.Index);
                            var (normalizedSource, normalizedTarget, reversed) = NormalizeDirection(edge.Source, edge.Target);
                            
                            return new
                            {
                                id = deterministicId,
                                index = edge.Index,
                                normalizedSource,
                                normalizedTarget,
                                relationship = edge.Relationship,
                                reversed
                            };
                        }).ToList();

                        var query = """
                            UNWIND $edges AS edgeData
                            MATCH (source:Node {id: edgeData.normalizedSource}), (target:Node {id: edgeData.normalizedTarget})
                            MERGE (source)-[r:RELATES_TO]->(target)
                            SET r.id = edgeData.id, r.index = edgeData.index, r.relationship = edgeData.relationship, r.reversed = edgeData.reversed
                            """;

                        await tx.RunAsync(query, new { edges = edgeDataList });
                        return Task.CompletedTask;
                    });
                    
                    _logger.LogDebug("Bulk inserted batch of {Count} edges", batch.Count);
                }
                
                _logger.LogInformation("Bulk inserted {TotalCount} edges successfully", edges.Count);
                return success;
            });
        }

        public async Task<bool> BulkUpsertNodesAsync(List<Nodes> nodes)
        {
            return await BulkInsertNodesAsync(nodes); // MERGE handles upsert
        }

        public async Task<bool> BulkUpsertEdgesAsync(List<Edges> edges)
        {
            return await BulkInsertEdgesAsync(edges); // MERGE handles upsert
        }

        // Connection management
        public async Task<bool> TestConnectionAsync()
        {
            try
            {
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                await session.ExecuteReadAsync(async tx =>
                {
                    await tx.RunAsync("RETURN 1 as test");
                    return Task.CompletedTask;
                });
                
                _logger.LogInformation("Neo4j connection test successful");
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Neo4j connection test failed");
                return false;
            }
        }

        public async Task InitializeAsync()
        {
            await EnsureConstraintsAsync();
            var connectionTest = await TestConnectionAsync();
            if (!connectionTest)
            {
                throw new InvalidOperationException("Failed to connect to Neo4j database");
            }
        }

        public async Task DisposeAsync()
        {
            await _driver.CloseAsync();
            _constraintSemaphore?.Dispose();
        }

        public void Dispose()
        {
            _driver?.Dispose();
            _constraintSemaphore?.Dispose();
        }
        
        public async Task<List<string>> GetAllIndicesAsync()
        {
            await EnsureConstraintsAsync();
            
            return await ExecuteWithRetryAsync(async () =>
            {
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var cursor = await tx.RunAsync("MATCH (n:Node) RETURN DISTINCT n.index as index");
                    
                    var indices = new List<string>();
                    await foreach (var record in cursor)
                    {
                        var index = record["index"].As<string>();
                        if (!string.IsNullOrEmpty(index))
                        {
                            indices.Add(index);
                        }
                    }
                    return indices;
                });
                
                return result;
            });
        }
    }
}