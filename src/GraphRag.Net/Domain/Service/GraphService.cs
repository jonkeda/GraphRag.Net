using GraphRag.Net.Domain.Interface;
using GraphRag.Net.Domain.Model.Graph;
using GraphRag.Net.Options;
using GraphRag.Net.Repositories;
using GraphRag.Net.Utils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Memory;
using Microsoft.SemanticKernel.Text;
using Newtonsoft.Json;

namespace GraphRag.Net.Domain.Service
{
    [ServiceDescription(typeof(IGraphService), ServiceLifetime.Scoped)]
    public class GraphService(
        IGraphRepository _graphRepository,
        ISemanticService _semanticService,
        ICommunityDetectionService _communityDetectionService
    ) : IGraphService
    {
        public List<string> GetAllIndex() =>
            _graphRepository.GetAllIndexesAsync().GetAwaiter().GetResult();

        public GraphViewModel GetAllGraphs(string index)
        {
            if (string.IsNullOrWhiteSpace(index)) throw new ArgumentException("Index required value cannot be null.");
            var nodes = _graphRepository.GetNodesByIndexAsync(index).GetAwaiter().GetResult();
            var edges = _graphRepository.GetEdgesByIndexAsync(index).GetAwaiter().GetResult();

            var vm = new GraphViewModel();
            var typeColor = new Dictionary<string, string>();
            var rand = new Random();

            foreach (var n in nodes)
            {
                var color = typeColor.TryGetValue(n.Type, out var c) ? c : (typeColor[n.Type] = $"#{rand.Next(0x1000000):X6}");
                vm.nodes.Add(new NodesViewModel
                {
                    id = n.Id,
                    text = n.Name,
                    color = color,
                    data = new NodesDataModel { desc = n.Desc.ConvertToString() }
                });
            }

            foreach (var e in edges)
            {
                vm.lines.Add(new LinesViewModel
                {
                    from = e.Source,
                    to = e.Target,
                    text = e.Relationship
                });
            }
            return vm;
        }

        public async Task InsertTextChunkAsync(string index, string input)
        {
            if (string.IsNullOrWhiteSpace(index) || string.IsNullOrWhiteSpace(input))
                throw new ArgumentException("Values required for index and input cannot be null.");

            var lines = TextChunker.SplitPlainTextLines(input, TextChunkerOption.LinesToken);
            var paragraphs = TextChunker.SplitPlainTextParagraphs(lines, TextChunkerOption.ParagraphsToken);
            var optimizedChunks = CreateOverlappingChunks(paragraphs);

            foreach (var chunk in optimizedChunks)
                await InsertGraphDataAsync(index, chunk);
        }

        private List<string> CreateOverlappingChunks(List<string> paragraphs)
        {
            var chunks = new List<string>();
            const int maxChunkSize = 3;
            const int overlap = 1;
            if (paragraphs.Count <= maxChunkSize)
            {
                chunks.Add(string.Join("\n\n", paragraphs));
                return chunks;
            }
            for (int i = 0; i < paragraphs.Count; i += (maxChunkSize - overlap))
            {
                var sub = paragraphs.Skip(i).Take(maxChunkSize).ToList();
                if (sub.Count == 0) break;
                var chunk = string.Join("\n\n", sub);
                if (!chunks.Contains(chunk)) chunks.Add(chunk);
                if (i + maxChunkSize >= paragraphs.Count) break;
            }
            return chunks;
        }

        public async Task InsertGraphDataAsync(string index, string input)
        {
            if (string.IsNullOrWhiteSpace(index) || string.IsNullOrWhiteSpace(input))
                throw new ArgumentException("Values required for index and input cannot be null.");

            try
            {
                var textMemory = await _semanticService.GetTextMemory();
                var graph = await _semanticService.CreateGraphAsync(input);
                var nodeIdMap = new Dictionary<string, string>();
                var newNodes = new List<Nodes>();

                var existingNodes = await _graphRepository.GetNodesByIndexAsync(index);

                foreach (var n in graph.Nodes)
                {
                    var descText = $"Name:{n.Name};Type:{n.Type};Desc:{n.Desc}";
                    var existing = existingNodes.FirstOrDefault(x => x.Name == n.Name);

                    if (existing != null && !string.IsNullOrWhiteSpace(n.Desc))
                    {
                        var merged = await _semanticService.MergeDesc(existing.Desc.ConvertToString(), n.Desc.ConvertToString());
                        existing.Desc = string.IsNullOrEmpty(merged)
                            ? existing.Desc.ConvertToString() + "; " + n.Desc.ConvertToString()
                            : merged;
                        await _graphRepository.UpdateNodeAsync(existing);
                        nodeIdMap[n.Id] = existing.Id;
                        await textMemory.SaveInformationAsync(index, existing.Id, descText.Replace(n.Desc.ConvertToString(), existing.Desc.ConvertToString()), cancellationToken: default);
                        continue;
                    }

                    bool mergedNode = false;
                    var potentialRel = new List<string>();
                    await foreach (var mem in textMemory.SearchAsync(index, descText, limit: 5, minRelevanceScore: 0.7))
                    {
                        if (mem.Relevance == 1)
                        {
                            nodeIdMap[n.Id] = mem.Metadata.Id;
                            mergedNode = true;
                            break;
                        }
                        potentialRel.Add(mem.Metadata.Id);
                    }
                    if (mergedNode) continue;

                    var node = new Nodes
                    {
                        Id = Guid.NewGuid().ToString(),
                        Index = index,
                        Name = n.Name,
                        Type = n.Type,
                        Desc = n.Desc.ConvertToString()
                    };
                    nodeIdMap[n.Id] = node.Id;
                    await _graphRepository.CreateNodeAsync(node);
                    newNodes.Add(node);

                    await textMemory.SaveInformationAsync(index, node.Id, descText, cancellationToken: default);

                    foreach (var candidateId in potentialRel)
                    {
                        var candidate = existingNodes.FirstOrDefault(x => x.Id == candidateId) ?? newNodes.FirstOrDefault(x => x.Id == candidateId);
                        if (candidate == null) continue;
                        var relation = await _semanticService.GetRelationship(
                            $"Name:{candidate.Name};Type:{candidate.Type};Desc:{candidate.Desc}",
                            descText);
                        if (relation.IsRelationship)
                        {
                            var source = relation.Edge.Source == "node1" ? candidate.Id : node.Id;
                            var target = relation.Edge.Source == "node1" ? node.Id : candidate.Id;
                            if (!await _graphRepository.EdgeExistsAsync(source, target))
                            {
                                await _graphRepository.CreateEdgeAsync(new Edges
                                {
                                    Id = Guid.NewGuid().ToString(),
                                    Index = index,
                                    Source = source,
                                    Target = target,
                                    Relationship = relation.Edge.Relationship
                                });
                            }
                        }
                    }
                }

                foreach (var e in graph.Edges)
                {
                    if (!nodeIdMap.ContainsKey(e.Source) || !nodeIdMap.ContainsKey(e.Target)) continue;
                    await _graphRepository.CreateEdgeAsync(new Edges
                    {
                        Id = Guid.NewGuid().ToString(),
                        Index = index,
                        Source = nodeIdMap[e.Source],
                        Target = nodeIdMap[e.Target],
                        Relationship = e.Relationship
                    });
                }

                await ProcessOrphanNodesAsync(index, newNodes);

                var allEdges = await _graphRepository.GetEdgesByIndexAsync(index);
                var dupGroups = allEdges.GroupBy(e => new { e.Source, e.Target }).Where(g => g.Count() > 1);

                foreach (var g in dupGroups)
                {
                    var list = g.ToList();
                    var primary = list.First();
                    foreach (var extra in list.Skip(1))
                    {
                        if (primary.Relationship == extra.Relationship)
                        {
                            await _graphRepository.DeleteEdgeAsync(extra.Id);
                            continue;
                        }
                        var mergedRel = await _semanticService.MergeDesc(primary.Relationship, extra.Relationship);
                        primary.Relationship = string.IsNullOrEmpty(mergedRel)
                            ? primary.Relationship + "; " + extra.Relationship
                            : mergedRel;
                        await _graphRepository.DeleteEdgeAsync(extra.Id);
                        await _graphRepository.CreateEdgeAsync(primary);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Insert graph data failed: {ex}");
            }
        }

        public async Task<GraphModel> SearchGraphModel(string index, string input)
        {
            if (string.IsNullOrWhiteSpace(index) || string.IsNullOrWhiteSpace(input))
                throw new ArgumentException("Values required for index and input cannot be null.");

            var textMemList = await RetrieveTextMemModelList(index, input);
            if (!textMemList.Any()) return new GraphModel();

            var seedIds = textMemList.Select(m => m.Id).ToList();
            var seedNodes = await _graphRepository.GetNodesByIdsAsync(seedIds);
            var weightMap = textMemList.ToDictionary(m => m.Id, m => m.Relevance);

            var graph = BuildRecursiveSubgraph(index, seedNodes, weightMap);
            var estTokens = EstimateTokenCount(graph);
            if (estTokens > GraphSearchOption.MaxTokens)
                graph = LimitGraphByTokenCount(graph, weightMap);
            return graph;
        }

        public async Task<string> SearchGraphAsync(string index, string input)
        {
            var graph = await SearchGraphModel(index, input);
            return await _semanticService.GetGraphAnswerAsync(JsonConvert.SerializeObject(graph), input);
        }

        public async IAsyncEnumerable<StreamingKernelContent> SearchGraphStreamAsync(string index, string input)
        {
            var graph = await SearchGraphModel(index, input);
            if (!graph.Nodes.Any()) yield break;
            var stream = _semanticService.GetGraphAnswerStreamAsync(JsonConvert.SerializeObject(graph), input);
            await foreach (var chunk in stream) yield return chunk;
        }

        public async Task GraphCommunitiesAsync(string index)
        {
            var nodes = await _graphRepository.GetNodesByIndexAsync(index);
            var edges = await _graphRepository.GetEdgesByIndexAsync(index);

            var oldComms = await _graphRepository.GetCommunitiesByIndexAsync(index);
            foreach (var c in oldComms) await _graphRepository.DeleteCommunityAsync(c.CommunitieId);
            var oldCn = await _graphRepository.GetCommunityNodesByIndexAsync(index);
            foreach (var cn in oldCn) await _graphRepository.DeleteCommunityNodeAsync(cn.CommunitieId, cn.NodeId);

            var graph = new Graph();
            foreach (var e in edges) graph.AddEdge(e.Source, e.Target);

            var labels = _communityDetectionService.FastLabelPropagationAlgorithm(graph);

            foreach (var kv in labels)
            {
                await _graphRepository.CreateCommunityNodeAsync(new CommunitieNodes
                {
                    Index = index,
                    CommunitieId = kv.Value,
                    NodeId = kv.Key
                });
            }

            var communityIds = labels.Values.Distinct().ToList();
            foreach (var cid in communityIds)
            {
                var links = await _graphRepository.GetCommunityNodesByCommunityAsync(cid);
                var memberNodes = await _graphRepository.GetNodesByIdsAsync(links.Select(l => l.NodeId).ToList());
                var descBlock = string.Join(Environment.NewLine, memberNodes.Select(n => $"Name:{n.Name}; Type:{n.Type}; Desc:{n.Desc}"));
                var summary = await _semanticService.CommunitySummaries(descBlock);
                await _graphRepository.CreateCommunityAsync(new Communities
                {
                    CommunitieId = cid,
                    Index = index,
                    Summaries = summary
                });
            }
        }

        public async Task GraphGlobalAsync(string index)
        {
            var communities = await _graphRepository.GetCommunitiesByIndexAsync(index);
            var combined = string.Join(Environment.NewLine, communities.Select(c => c.Summaries));
            var global = await _semanticService.GlobalSummaries(combined);
            await _graphRepository.UpsertGlobalAsync(new Globals
            {
                Index = index,
                Summaries = global
            });
        }

        public async Task DeleteGraph(string index)
        {
            var memory = await _semanticService.GetTextMemory();
            var nodes = await _graphRepository.GetNodesByIndexAsync(index);
            foreach (var n in nodes)
                await memory.RemoveAsync(index, n.Id);
            await _graphRepository.BulkDeleteIndexAsync(index);
        }

        #region Internal helpers

        private async Task ProcessOrphanNodesAsync(string index, List<Nodes> newNodes)
        {
            var edges = await _graphRepository.GetEdgesByIndexAsync(index);
            var connectedIds = edges.Select(e => e.Source)
                                    .Concat(edges.Select(e => e.Target))
                                    .ToHashSet();
            foreach (var node in newNodes)
            {
                if (!connectedIds.Contains(node.Id))
                    await AttemptConnectOrphanNodeAsync(index, node);
            }
        }

        private async Task<int> AttemptConnectOrphanNodeAsync(string index, Nodes orphanNode)
        {
            int created = 0;
            var textMemory = await _semanticService.GetTextMemory();
            string nodeText = $"Name:{orphanNode.Name};Type:{orphanNode.Type};Desc:{orphanNode.Desc}";
            var candidates = new HashSet<string>();

            await foreach (var mem in textMemory.SearchAsync(index, nodeText, limit: 10, minRelevanceScore: 0.5))
            {
                if (mem.Metadata.Id != orphanNode.Id) candidates.Add(mem.Metadata.Id);
            }

            if (candidates.Count < 3)
            {
                await foreach (var mem in textMemory.SearchAsync(index, orphanNode.Name, limit: 5, minRelevanceScore: 0.6))
                {
                    if (mem.Metadata.Id != orphanNode.Id) candidates.Add(mem.Metadata.Id);
                }
            }

            var candidateNodes = await _graphRepository.GetNodesByIdsAsync(candidates.Take(10).ToList());
            foreach (var candidate in candidateNodes.Take(5))
            {
                var relation = await _semanticService.GetRelationship(
                    $"Name:{candidate.Name};Type:{candidate.Type};Desc:{candidate.Desc}",
                    nodeText);
                if (!relation.IsRelationship) continue;
                var source = relation.Edge.Source == "node1" ? candidate.Id : orphanNode.Id;
                var target = relation.Edge.Source == "node1" ? orphanNode.Id : candidate.Id;
                if (!await _graphRepository.EdgeExistsAsync(source, target))
                {
                    await _graphRepository.CreateEdgeAsync(new Edges
                    {
                        Id = Guid.NewGuid().ToString(),
                        Index = index,
                        Source = source,
                        Target = target,
                        Relationship = relation.Edge.Relationship
                    });
                    if (++created >= 2) break;
                }
            }
            return created;
        }

        private async Task<List<TextMemModel>> RetrieveTextMemModelList(string index, string input, double? minRel = null, int? limit = null)
        {
            var textMemory = await _semanticService.GetTextMemory();
            var results = new List<TextMemModel>();
            double threshold = minRel ?? GraphSearchOption.SearchMinRelevance;
            int lim = limit ?? GraphSearchOption.SearchLimit;

            int count = 0;
            await foreach (var r in textMemory.SearchAsync(index, input, limit: lim, minRelevanceScore: threshold))
            {
                count++;
                results.Add(new TextMemModel { Id = r.Metadata.Id, Text = r.Metadata.Text, Relevance = r.Relevance });
            }

            if (count < 2 && threshold > 0.3)
            {
                double lower = Math.Max(0.3, threshold - 0.2);
                await foreach (var r in textMemory.SearchAsync(index, input, limit: lim + 2, minRelevanceScore: lower))
                {
                    if (!results.Any(x => x.Id == r.Metadata.Id))
                        results.Add(new TextMemModel { Id = r.Metadata.Id, Text = r.Metadata.Text, Relevance = r.Relevance });
                }
            }

            return results.OrderByDescending(x => x.Relevance).ToList();
        }

        private GraphModel BuildRecursiveSubgraph(string index, List<Nodes> seed, Dictionary<string, double> weights)
        {
            var allNodes = new List<Nodes>(seed);
            var allEdges = new List<Edges>();
            var frontier = new List<Nodes>(seed);
            int depth = 0;

            while (frontier.Count > 0 && depth < GraphSearchOption.NodeDepth && allNodes.Count < GraphSearchOption.MaxNodes)
            {
                frontier = frontier
                    .OrderByDescending(n => weights.GetValueOrDefault(n.Id, 0))
                    .Take(5)
                    .ToList();

                var candidateIds = allNodes.Select(n => n.Id).Concat(frontier.Select(n => n.Id)).Distinct().ToList();
                var edges = _graphRepository.GetEdgesByNodeIdsAsync(index, candidateIds).GetAwaiter().GetResult();

                foreach (var e in edges)
                {
                    if (!allEdges.Any(x => x.Source == e.Source && x.Target == e.Target))
                        allEdges.Add(e);
                }

                var newNodeIds = edges.SelectMany(e => new[] { e.Source, e.Target })
                    .Distinct()
                    .Where(id => allNodes.All(n => n.Id != id))
                    .ToList();

                if (newNodeIds.Count == 0) break;

                var newNodes = _graphRepository.GetNodesByIdsAsync(newNodeIds).GetAwaiter().GetResult();
                foreach (var nn in newNodes)
                {
                    if (!weights.ContainsKey(nn.Id))
                        weights[nn.Id] = weights.Values.DefaultIfEmpty(0).Max() * 0.8;
                }
                allNodes.AddRange(newNodes);
                frontier = newNodes;
                depth++;
            }

            if (allNodes.Count > GraphSearchOption.MaxNodes)
            {
                allNodes = allNodes.OrderByDescending(n => weights.GetValueOrDefault(n.Id, 0))
                    .Take(GraphSearchOption.MaxNodes).ToList();
                allEdges = allEdges.Where(e => allNodes.Any(n => n.Id == e.Source) && allNodes.Any(n => n.Id == e.Target)).ToList();
            }

            return new GraphModel { Nodes = allNodes, Edges = allEdges };
        }

        private int EstimateTokenCount(GraphModel model)
        {
            int tokens = 200;
            foreach (var n in model.Nodes)
            {
                string desc = n.Desc ?? "";
                int chinese = desc.Count(c => c >= 0x4E00 && c <= 0x9FFF);
                int other = desc.Length - chinese;
                tokens += chinese + (int)(other * 0.75);
                tokens += (n.Id?.Length ?? 0) / 3 + (n.Name?.Length ?? 0) / 3 + 15;
            }
            tokens += model.Edges.Count * 10;
            return tokens;
        }

        private GraphModel LimitGraphByTokenCount(GraphModel model, Dictionary<string, double> weights)
        {
            var selected = new List<Nodes>();
            int budget = 200;
            foreach (var n in model.Nodes.OrderByDescending(x => weights.GetValueOrDefault(x.Id, 0)))
            {
                string desc = n.Desc ?? "";
                int chinese = desc.Count(c => c >= 0x4E00 && c <= 0x9FFF);
                int other = desc.Length - chinese;
                int nodeTokens = chinese + (int)(other * 0.75) + (n.Id?.Length ?? 0) / 3 + (n.Name?.Length ?? 0) / 3 + 15;
                if (budget + nodeTokens > GraphSearchOption.MaxTokens * 0.9) continue;
                selected.Add(n);
                budget += nodeTokens;
            }
            var edges = model.Edges
                .Where(e => selected.Any(n => n.Id == e.Source) && selected.Any(n => n.Id == e.Target))
                .ToList();
            return new GraphModel { Nodes = selected, Edges = edges };
        }

        #endregion
    }
}