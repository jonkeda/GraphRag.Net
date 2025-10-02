using GraphRag.Net.Repositories;

namespace GraphRag.Net.Base
{
    /// <summary>
    /// 图数据库统一接口，支持多种后端存储
    /// </summary>
    public interface IGraphRepository
    {
        // Nodes operations
        Task<List<Nodes>> GetNodesAsync(string index);
        Task<List<Nodes>> GetNodesByIdsAsync(string index, List<string> nodeIds);
        Task<bool> InsertNodeAsync(Nodes node);
        Task<bool> InsertNodesAsync(List<Nodes> nodes);
        Task<bool> UpdateNodeAsync(Nodes node);
        Task<bool> DeleteNodeAsync(string nodeId);
        Task<bool> NodeExistsAsync(string nodeId);
        Task<Nodes?> GetNodeByIdAsync(string nodeId);

        // Edges operations
        Task<List<Edges>> GetEdgesAsync(string index);
        Task<List<Edges>> GetEdgesBySourceAsync(string index, string sourceId);
        Task<List<Edges>> GetEdgesByTargetAsync(string index, string targetId);
        Task<List<Edges>> GetEdgesByNodeAsync(string index, string nodeId);
        Task<bool> InsertEdgeAsync(Edges edge);
        Task<bool> InsertEdgesAsync(List<Edges> edges);
        Task<bool> UpdateEdgeAsync(Edges edge);
        Task<bool> DeleteEdgeAsync(string edgeId);
        Task<bool> EdgeExistsAsync(string sourceId, string targetId, string index);
        Task<Edges?> GetEdgeByIdAsync(string edgeId);

        // Graph operations
        Task<(List<Nodes> nodes, List<Edges> edges)> GetGraphAsync(string index);
        Task<(List<Nodes> nodes, List<Edges> edges)> GetSubGraphAsync(string index, List<string> nodeIds, int maxDepth = 1);
        
        // Batch operations
        Task<bool> BulkInsertNodesAsync(List<Nodes> nodes);
        Task<bool> BulkInsertEdgesAsync(List<Edges> edges);
        Task<bool> BulkUpsertNodesAsync(List<Nodes> nodes);
        Task<bool> BulkUpsertEdgesAsync(List<Edges> edges);

        // Connection management
        Task<bool> TestConnectionAsync();
        Task InitializeAsync();
        Task DisposeAsync();
        
        // Utility methods
        Task<List<string>> GetAllIndicesAsync();
    }
}