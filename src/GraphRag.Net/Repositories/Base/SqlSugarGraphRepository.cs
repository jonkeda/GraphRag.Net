using GraphRag.Net.Repositories;
using Microsoft.Extensions.Logging;
using System.Linq.Expressions;

namespace GraphRag.Net.Base
{
    /// <summary>
    /// SqlSugar实现的图数据库存储，包装现有的仓储
    /// </summary>
    public class SqlSugarGraphRepository : IGraphRepository
    {
        internal readonly INodes_Repositories _nodesRepository;
        internal readonly IEdges_Repositories _edgesRepository;
        private readonly ILogger<SqlSugarGraphRepository> _logger;

        public SqlSugarGraphRepository(
            INodes_Repositories nodesRepository,
            IEdges_Repositories edgesRepository,
            ILogger<SqlSugarGraphRepository> logger)
        {
            _nodesRepository = nodesRepository;
            _edgesRepository = edgesRepository;
            _logger = logger;
        }

        // Nodes operations
        public async Task<List<Nodes>> GetNodesAsync(string index)
        {
            return await _nodesRepository.GetListAsync(n => n.Index == index);
        }

        public async Task<List<Nodes>> GetNodesByIdsAsync(string index, List<string> nodeIds)
        {
            return await _nodesRepository.GetListAsync(n => n.Index == index && nodeIds.Contains(n.Id));
        }

        public async Task<bool> InsertNodeAsync(Nodes node)
        {
            return await _nodesRepository.InsertAsync(node);
        }

        public async Task<bool> InsertNodesAsync(List<Nodes> nodes)
        {
            return await _nodesRepository.InsertRangeAsync(nodes);
        }

        public async Task<bool> UpdateNodeAsync(Nodes node)
        {
            return await _nodesRepository.UpdateAsync(node);
        }

        public async Task<bool> DeleteNodeAsync(string nodeId)
        {
            return await _nodesRepository.DeleteAsync(nodeId);
        }

        public async Task<bool> NodeExistsAsync(string nodeId)
        {
            return await _nodesRepository.IsAnyAsync(n => n.Id == nodeId);
        }

        public async Task<Nodes?> GetNodeByIdAsync(string nodeId)
        {
            try
            {
                return await _nodesRepository.GetByIdAsync(nodeId);
            }
            catch
            {
                return null;
            }
        }

        // Edges operations
        public async Task<List<Edges>> GetEdgesAsync(string index)
        {
            return await _edgesRepository.GetListAsync(e => e.Index == index);
        }

        public async Task<List<Edges>> GetEdgesBySourceAsync(string index, string sourceId)
        {
            return await _edgesRepository.GetListAsync(e => e.Index == index && e.Source == sourceId);
        }

        public async Task<List<Edges>> GetEdgesByTargetAsync(string index, string targetId)
        {
            return await _edgesRepository.GetListAsync(e => e.Index == index && e.Target == targetId);
        }

        public async Task<List<Edges>> GetEdgesByNodeAsync(string index, string nodeId)
        {
            return await _edgesRepository.GetListAsync(e => e.Index == index && (e.Source == nodeId || e.Target == nodeId));
        }

        public async Task<bool> InsertEdgeAsync(Edges edge)
        {
            return await _edgesRepository.InsertAsync(edge);
        }

        public async Task<bool> InsertEdgesAsync(List<Edges> edges)
        {
            return await _edgesRepository.InsertRangeAsync(edges);
        }

        public async Task<bool> UpdateEdgeAsync(Edges edge)
        {
            return await _edgesRepository.UpdateAsync(edge);
        }

        public async Task<bool> DeleteEdgeAsync(string edgeId)
        {
            return await _edgesRepository.DeleteAsync(edgeId);
        }

        public async Task<bool> EdgeExistsAsync(string sourceId, string targetId, string index)
        {
            return await _edgesRepository.IsAnyAsync(e => e.Source == sourceId && e.Target == targetId && e.Index == index);
        }

        public async Task<Edges?> GetEdgeByIdAsync(string edgeId)
        {
            try
            {
                return await _edgesRepository.GetByIdAsync(edgeId);
            }
            catch
            {
                return null;
            }
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
            var allNodes = new HashSet<string>(nodeIds);
            var allEdges = new List<Edges>();

            // Start with the initial nodes
            var currentDepth = 0;
            var currentNodes = new HashSet<string>(nodeIds);

            while (currentDepth < maxDepth && currentNodes.Any())
            {
                // Get all edges connected to current nodes
                var edges = await _edgesRepository.GetListAsync(e => 
                    e.Index == index && 
                    (currentNodes.Contains(e.Source) || currentNodes.Contains(e.Target)));

                allEdges.AddRange(edges);

                // Find new nodes for next iteration
                var newNodes = new HashSet<string>();
                foreach (var edge in edges)
                {
                    if (!allNodes.Contains(edge.Source))
                        newNodes.Add(edge.Source);
                    if (!allNodes.Contains(edge.Target))
                        newNodes.Add(edge.Target);
                }

                allNodes.UnionWith(newNodes);
                currentNodes = newNodes;
                currentDepth++;
            }

            var nodes = await GetNodesByIdsAsync(index, allNodes.ToList());
            return (nodes, allEdges.Distinct().ToList());
        }

        // Batch operations
        public async Task<bool> BulkInsertNodesAsync(List<Nodes> nodes)
        {
            return await InsertNodesAsync(nodes);
        }

        public async Task<bool> BulkInsertEdgesAsync(List<Edges> edges)
        {
            return await InsertEdgesAsync(edges);
        }

        public async Task<bool> BulkUpsertNodesAsync(List<Nodes> nodes)
        {
            var result = true;
            foreach (var node in nodes)
            {
                var exists = await NodeExistsAsync(node.Id);
                if (exists)
                {
                    result &= await UpdateNodeAsync(node);
                }
                else
                {
                    result &= await InsertNodeAsync(node);
                }
            }
            return result;
        }

        public async Task<bool> BulkUpsertEdgesAsync(List<Edges> edges)
        {
            var result = true;
            foreach (var edge in edges)
            {
                var exists = await EdgeExistsAsync(edge.Source, edge.Target, edge.Index);
                if (exists)
                {
                    result &= await UpdateEdgeAsync(edge);
                }
                else
                {
                    result &= await InsertEdgeAsync(edge);
                }
            }
            return result;
        }

        // Connection management
        public async Task<bool> TestConnectionAsync()
        {
            try
            {
                var count = await _nodesRepository.CountAsync(n => true);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to test SqlSugar connection");
                return false;
            }
        }

        public async Task InitializeAsync()
        {
            // SqlSugar initialization is handled in CodeFirst method
            await Task.CompletedTask;
        }

        public async Task DisposeAsync()
        {
            // SqlSugar disposal is handled by DI container
            await Task.CompletedTask;
        }
        
        public async Task<List<string>> GetAllIndicesAsync()
        {
            var indexs = _nodesRepository.GetDB().Queryable<Nodes>().GroupBy(p => p.Index).Select(p => p.Index).ToList();
            return await Task.FromResult(indexs);
        }
    }
}