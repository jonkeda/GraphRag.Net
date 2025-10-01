using System.Collections.Generic;
using System.Threading.Tasks;

namespace GraphRag.Net.Repositories
{
    /// <summary>
    /// Unified abstraction over graph persistence (Nodes, Edges, Communities, Community-Node links, Global summaries).
    /// </summary>
    public interface IGraphRepository
    {
        // Index scope
        Task<List<string>> GetAllIndexesAsync();
        Task<bool> BulkDeleteIndexAsync(string index);

        // Nodes
        Task<bool> CreateNodeAsync(Nodes node);
        Task<bool> BulkCreateNodesAsync(List<Nodes> nodes);
        Task<Nodes?> GetNodeByIdAsync(string id);
        Task<List<Nodes>> GetNodesByIndexAsync(string index);
        Task<List<Nodes>> GetNodesByIdsAsync(List<string> ids);
        Task<bool> UpdateNodeAsync(Nodes node);
        Task<bool> DeleteNodeAsync(string id);
        Task<bool> NodeExistsAsync(string id);

        // Edges
        Task<bool> CreateEdgeAsync(Edges edge);
        Task<bool> BulkCreateEdgesAsync(List<Edges> edges);
        Task<Edges?> GetEdgeByIdAsync(string id);
        Task<List<Edges>> GetEdgesByIndexAsync(string index);
        Task<List<Edges>> GetEdgesBySourceAsync(string sourceId);
        Task<List<Edges>> GetEdgesByTargetAsync(string targetId);
        Task<List<Edges>> GetEdgesByNodeIdsAsync(string index, List<string> nodeIds);
        Task<bool> EdgeExistsAsync(string sourceId, string targetId, string? relationship = null);
        Task<bool> DeleteEdgeAsync(string id);
        Task<bool> BulkDeleteEdgesByIndexAsync(string index);

        // Communities
        Task<bool> CreateCommunityAsync(Communities c);
        Task<Communities?> GetCommunityByIdAsync(string id);
        Task<List<Communities>> GetCommunitiesByIndexAsync(string index);
        Task<bool> UpdateCommunityAsync(Communities c);
        Task<bool> DeleteCommunityAsync(string id);

        // Community ↔ Nodes
        Task<bool> CreateCommunityNodeAsync(CommunitieNodes cn);
        Task<List<CommunitieNodes>> GetCommunityNodesByIndexAsync(string index);
        Task<List<CommunitieNodes>> GetCommunityNodesByCommunityAsync(string communityId);
        Task<List<CommunitieNodes>> GetCommunityNodesByNodeAsync(string nodeId);
        Task<bool> DeleteCommunityNodeAsync(string communityId, string nodeId);

        // Globals
        Task<Globals?> GetGlobalByIndexAsync(string index);
        Task<bool> UpsertGlobalAsync(Globals g);
        Task<bool> DeleteGlobalByIndexAsync(string index);

        // Utility
        Task<List<Nodes>> GetConnectedNodesAsync(string nodeId);
    }
}