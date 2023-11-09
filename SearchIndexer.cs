using Azure.Search.Documents.Indexes;
using Azure.Search.Documents.Indexes.Models;
using Bupa.CommonPlatforms.FindAndBook.Indexer.Models;
using Bupa.CommonPlatforms.FindAndBook.Indexer.Services.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Bupa.CommonPlatforms.FindAndBook.Indexer.Services
{
    public class SearchIndexerService : ISearchIndexerService
    {
        private readonly Settings _settings;
        private readonly SearchIndexerClient _searchIndexerClient;
        private readonly ILogger<SearchIndexerService> _logger;

        public SearchIndexerService(IOptions<Settings> options, SearchIndexerClient searchIndexerClient, ILogger<SearchIndexerService> logger)
        {
            _settings = options.Value;
            _searchIndexerClient = searchIndexerClient;
            _logger = logger;
        }

        public async Task CreateAndRunIndexer(string indexName, string dataSourceName)
        {
            try
            {
                IndexingParameters parameters = new IndexingParameters()
                {
                    IndexingParametersConfiguration = new IndexingParametersConfiguration()
                };
                parameters.IndexingParametersConfiguration.Add("parsingMode", "json");
                SearchIndexer blobIndexer = new SearchIndexer(
                    name: indexName,
                    dataSourceName: dataSourceName,
                    targetIndexName: indexName)
                {
                    Parameters = parameters,
                    Schedule = new IndexingSchedule(TimeSpan.FromDays(1))
                };
                await _searchIndexerClient.CreateOrUpdateIndexerAsync(blobIndexer);
                await _searchIndexerClient.RunIndexerAsync(blobIndexer.Name);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Exception thrown in {nameof(CreateAndRunIndexer)}: {ex.Message}");
                throw;
            }
        }

        public async Task CreateOrUpdateIndexerDataSource(string blobContainerName)
        {
            try
            {
                SearchIndexerDataSourceConnection sourceConnection = new SearchIndexerDataSourceConnection(
                    name: $"{blobContainerName}-indexer",
                    type: SearchIndexerDataSourceType.AzureBlob,
                    connectionString: _settings.BlobStorageConnectionString,
                    container: new SearchIndexerDataContainer(blobContainerName));

                await _searchIndexerClient.CreateOrUpdateDataSourceConnectionAsync(sourceConnection);
            }
            catch (Exception ex)
            {
                _logger.LogInformation($"Exception thrown in {nameof(CreateOrUpdateIndexerDataSource)}: {ex.Message}");
                throw;
            }
        }
    }
}
