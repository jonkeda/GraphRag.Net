using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace GraphRag.Net.Repositories
{
    /// <summary>
    /// SqlSugar adapter implementation of IGraphRepository wrapping existing repository interfaces.
    /// </summary>
    public class SqlSugarGraphRepository : IGraphRepository
    {
        private readonly INodes_Repositories _nodes;
        private readonly IEdges_Repositories _edges;
        private readonly ICommunities_Repositories _communities;
        private readonly ICommunitieNodes_Repositories _communityNodes;
        private readonly IGlobals_Repositories _globals;

        public SqlSugarGraphRepository(
            INodes_Repositories nodes,
            IEdges_Repositories edges,
            ICommunities_Repositories communities,
            ICommunitieNodes_Repositories communityNodes,
            IGlobals_Repositories globals)
        {
            _nodes = nodes;
            _edges = edges;
            _communities = communities;
            _communityNodes = communityNodes;
            _globals = globals;
        }

        public Task<List<string>> GetAllIndexesAsync() => Task.FromResult(_nodes.GetDB().Queryable<Nodes>()
            .GroupBy(p => p.Index).Select(p => p.Index).ToList());

        public async Task<bool> BulkDeleteIndexAsync(string index)
        {
            await _globals.DeleteAsync(g => g.Index == index);
            await _communities.DeleteAsync(c => c.Index == index);
            await _communityNodes.DeleteAsync(cn => cn.Index == index);
            await _edges.DeleteAsync(e => e.Index == index);
            await _nodes.DeleteAsync(n => n.Index == index);
            return true;
        }

        // Nodes
        public Task<bool> CreateNodeAsync(Nodes node) => _nodes.InsertAsync(node);
        public async Task<bool> BulkCreateNodesAsync(List<Nodes> nodes) => nodes.Count == 0 || await _nodes.InsertRangeAsync(nodes);
        public Task<Nodes?> GetNodeByIdAsync(string id) => _nodes.GetByIdAsync(id);
        public Task<List<Nodes>> GetNodesByIndexAsync(string index) => _nodes.GetListAsync(n => n.Index == index);
        public Task<List<Nodes>> GetNodesByIdsAsync(List<string> ids) => _nodes.GetListAsync(n => ids.Contains(n.Id));
        public Task<bool> UpdateNodeAsync(Nodes node) => _nodes.UpdateAsync(node);
        public Task<bool> DeleteNodeAsync(string id) => _nodes.DeleteAsync(id);
        public Task<bool> NodeExistsAsync(string id) => _nodes.IsAnyAsync(n => n.Id == id);

        // Edges
        public Task<bool> CreateEdgeAsync(Edges edge) => _edges.InsertAsync(edge);
        public async Task<bool> BulkCreateEdgesAsync(List<Edges> edges) => edges.Count == 0 || await _edges.InsertRangeAsync(edges);
        public Task<Edges?> GetEdgeByIdAsync(string id) => _edges.GetByIdAsync(id);
        public Task<List<Edges>> GetEdgesByIndexAsync(string index) => _edges.GetListAsync(e => e.Index == index);
        public Task<List<Edges>> GetEdgesBySourceAsync(string sourceId) => _edges.GetListAsync(e => e.Source == sourceId);
        public Task<List<Edges>> GetEdgesByTargetAsync(string targetId) => _edges.GetListAsync(e => e.Target == targetId);
        public Task<List<Edges>> GetEdgesByNodeIdsAsync(string index, List<string> nodeIds) => _edges.GetListAsync(e => e.Index == index && nodeIds.Contains(e.Source) && nodeIds.Contains(e.Target));
        public Task<bool> EdgeExistsAsync(string sourceId, string targetId, string? relationship = null) => _edges.IsAnyAsync(e => e.Source == sourceId && e.Target == targetId && (relationship == null || e.Relationship == relationship));
        public Task<bool> DeleteEdgeAsync(string id) => _edges.DeleteAsync(id);
        public async Task<bool> BulkDeleteEdgesByIndexAsync(string index)
        {
            var ids = (await _edges.GetListAsync(e => e.Index == index)).Select(e => (dynamic)e.Id).ToArray();
            return ids.Length == 0 || await _edges.DeleteByIdsAsync(ids);
        }

        // Communities
        public Task<bool> CreateCommunityAsync(Communities c) => _communities.InsertAsync(c);
        public Task<Communities?> GetCommunityByIdAsync(string id) => _communities.GetByIdAsync(id);
        public Task<List<Communities>> GetCommunitiesByIndexAsync(string index) => _communities.GetListAsync(c => c.Index == index);
        public Task<bool> UpdateCommunityAsync(Communities c) => _communities.UpdateAsync(c);
        public Task<bool> DeleteCommunityAsync(string id) => _communities.DeleteAsync(id);

        // Community ↔ Nodes
        public Task<bool> CreateCommunityNodeAsync(CommunitieNodes cn) => _communityNodes.InsertAsync(cn);
        public Task<List<CommunitieNodes>> GetCommunityNodesByIndexAsync(string index) => _communityNodes.GetListAsync(cn => cn.Index == index);
        public Task<List<CommunitieNodes>> GetCommunityNodesByCommunityAsync(string communityId) => _communityNodes.GetListAsync(cn => cn.CommunitieId == communityId);
        public Task<List<CommunitieNodes>> GetCommunityNodesByNodeAsync(string nodeId) => _communityNodes.GetListAsync(cn => cn.NodeId == nodeId);
        public async Task<bool> DeleteCommunityNodeAsync(string communityId, string nodeId)
        {
            var list = await _communityNodes.GetListAsync(cn => cn.CommunitieId == communityId && cn.NodeId == nodeId);
            bool ok = true;
            foreach (var item in list) ok &= await _communityNodes.DeleteAsync(item);
            return ok;
        }

        // Globals
        public Task<Globals?> GetGlobalByIndexAsync(string index) => _globals.GetFirstAsync(g => g.Index == index);
        public async Task<bool> UpsertGlobalAsync(Globals g)
        {
            var existing = await _globals.GetFirstAsync(x => x.Index == g.Index);
            if (existing == null) return await _globals.InsertAsync(g);
            existing.Summaries = g.Summaries;
            return await _globals.UpdateAsync(existing);
        }
        public Task<bool> DeleteGlobalByIndexAsync(string index) => _globals.DeleteAsync(g => g.Index == index);

        // Utility
        public async Task<List<Nodes>> GetConnectedNodesAsync(string nodeId)
        {
            var edges = await _edges.GetListAsync(e => e.Source == nodeId || e.Target == nodeId);
            var neighborIds = edges.Select(e => e.Source == nodeId ? e.Target : e.Source).Distinct().ToList();
            if (neighborIds.Count == 0) return new List<Nodes>();
            return await _nodes.GetListAsync(n => neighborIds.Contains(n.Id));
        }
    }
}