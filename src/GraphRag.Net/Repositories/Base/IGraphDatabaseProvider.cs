namespace GraphRag.Net.Repositories
{
    public interface IGraphDatabaseProvider
    {
        // Define methods for database operations
        void Connect();
        void Disconnect();
        void ExecuteQuery(string query);
        // Add more methods as necessary
    }
}