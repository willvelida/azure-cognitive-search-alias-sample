@description('The suffix applied to deployed resources')
param suffix string = uniqueString(resourceGroup().id)

@description('The name of the Azure Cognitive Search service that will be deployed')
param searchServiceName string = 'search-${suffix}-dev'

@description('The pricing tier of the search service that will be deployed. Default is free')
param sku string = 'free'

@description('The location of the Azure Cognitive Search service that will be deployed. Default is location of the resource group')
param location string = resourceGroup().location

@description('Replicas distribute search workloads across the service.')
param replicaCount int = 1

@description('Partitions allow for scaling of document count as well as faster indexing by sharding your index over multiple search units')
param partitionCount int = 1

resource searchService 'Microsoft.Search/searchServices@2022-09-01' = {
  name: searchServiceName
  location: location
  sku: {
    name: sku
  }
  properties: {
    replicaCount: replicaCount
    partitionCount: partitionCount
  }
}
