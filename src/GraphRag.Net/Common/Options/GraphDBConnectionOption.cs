namespace GraphRag.Net.Options
{
    public class GraphDBConnectionOption
    {
        /// <summary>
        /// 数据库类型 (Sqlite, Neo4j)
        /// </summary>
        public static string DbType { get; set; } = "Sqlite";

        /// <summary>
        /// 业务数据链接字符串
        /// For Sqlite: Data Source=graph.db
        /// For Neo4j: bolt://localhost:7687
        /// </summary>
        public static string DBConnection { get; set; } = $"Data Source=graph.db";

        /// <summary>
        /// Neo4j用户名
        /// </summary>
        public static string Neo4jUsername { get; set; } = "neo4j";

        /// <summary>
        /// Neo4j密码
        /// </summary>
        public static string Neo4jPassword { get; set; } = "password";

        /// <summary>
        /// Neo4j数据库名
        /// </summary>
        public static string Neo4jDatabase { get; set; } = "neo4j";
        /// <summary>
        /// 向量数据连接字符串
        /// </summary>
        public static string VectorConnection { get; set; } = "graphmem.db";
        /// <summary>
        /// 向量数据维度，PG需要设置
        /// </summary>
        public static int VectorSize { get; set; } = 1536;
    }

}
