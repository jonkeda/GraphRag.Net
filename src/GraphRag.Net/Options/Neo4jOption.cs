using System;

namespace GraphRag.Net.Options
{
    /// <summary>
    /// Neo4j connection options
    /// </summary>
    public class Neo4jOption
    {
        public string Uri { get; set; } = string.Empty;
        public string User { get; set; } = string.Empty;
        public string Password { get; set; } = string.Empty;
        public string Database { get; set; } = "neo4j";
    }
}