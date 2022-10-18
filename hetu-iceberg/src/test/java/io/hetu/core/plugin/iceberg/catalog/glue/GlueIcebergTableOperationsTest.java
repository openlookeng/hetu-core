/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hetu.core.plugin.iceberg.catalog.glue;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionResult;
import com.amazonaws.services.glue.model.BatchDeleteConnectionRequest;
import com.amazonaws.services.glue.model.BatchDeleteConnectionResult;
import com.amazonaws.services.glue.model.BatchDeletePartitionRequest;
import com.amazonaws.services.glue.model.BatchDeletePartitionResult;
import com.amazonaws.services.glue.model.BatchDeleteTableRequest;
import com.amazonaws.services.glue.model.BatchDeleteTableResult;
import com.amazonaws.services.glue.model.BatchDeleteTableVersionRequest;
import com.amazonaws.services.glue.model.BatchDeleteTableVersionResult;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionResult;
import com.amazonaws.services.glue.model.BatchStopJobRunRequest;
import com.amazonaws.services.glue.model.BatchStopJobRunResult;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.ConcurrentModificationException;
import com.amazonaws.services.glue.model.CreateClassifierRequest;
import com.amazonaws.services.glue.model.CreateClassifierResult;
import com.amazonaws.services.glue.model.CreateConnectionRequest;
import com.amazonaws.services.glue.model.CreateConnectionResult;
import com.amazonaws.services.glue.model.CreateCrawlerRequest;
import com.amazonaws.services.glue.model.CreateCrawlerResult;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateDatabaseResult;
import com.amazonaws.services.glue.model.CreateDevEndpointRequest;
import com.amazonaws.services.glue.model.CreateDevEndpointResult;
import com.amazonaws.services.glue.model.CreateJobRequest;
import com.amazonaws.services.glue.model.CreateJobResult;
import com.amazonaws.services.glue.model.CreatePartitionRequest;
import com.amazonaws.services.glue.model.CreatePartitionResult;
import com.amazonaws.services.glue.model.CreateScriptRequest;
import com.amazonaws.services.glue.model.CreateScriptResult;
import com.amazonaws.services.glue.model.CreateSecurityConfigurationRequest;
import com.amazonaws.services.glue.model.CreateSecurityConfigurationResult;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateTableResult;
import com.amazonaws.services.glue.model.CreateTriggerRequest;
import com.amazonaws.services.glue.model.CreateTriggerResult;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.DeleteClassifierRequest;
import com.amazonaws.services.glue.model.DeleteClassifierResult;
import com.amazonaws.services.glue.model.DeleteConnectionRequest;
import com.amazonaws.services.glue.model.DeleteConnectionResult;
import com.amazonaws.services.glue.model.DeleteCrawlerRequest;
import com.amazonaws.services.glue.model.DeleteCrawlerResult;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseResult;
import com.amazonaws.services.glue.model.DeleteDevEndpointRequest;
import com.amazonaws.services.glue.model.DeleteDevEndpointResult;
import com.amazonaws.services.glue.model.DeleteJobRequest;
import com.amazonaws.services.glue.model.DeleteJobResult;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeletePartitionResult;
import com.amazonaws.services.glue.model.DeleteResourcePolicyRequest;
import com.amazonaws.services.glue.model.DeleteResourcePolicyResult;
import com.amazonaws.services.glue.model.DeleteSecurityConfigurationRequest;
import com.amazonaws.services.glue.model.DeleteSecurityConfigurationResult;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteTableResult;
import com.amazonaws.services.glue.model.DeleteTableVersionRequest;
import com.amazonaws.services.glue.model.DeleteTableVersionResult;
import com.amazonaws.services.glue.model.DeleteTriggerRequest;
import com.amazonaws.services.glue.model.DeleteTriggerResult;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetCatalogImportStatusRequest;
import com.amazonaws.services.glue.model.GetCatalogImportStatusResult;
import com.amazonaws.services.glue.model.GetClassifierRequest;
import com.amazonaws.services.glue.model.GetClassifierResult;
import com.amazonaws.services.glue.model.GetClassifiersRequest;
import com.amazonaws.services.glue.model.GetClassifiersResult;
import com.amazonaws.services.glue.model.GetConnectionRequest;
import com.amazonaws.services.glue.model.GetConnectionResult;
import com.amazonaws.services.glue.model.GetConnectionsRequest;
import com.amazonaws.services.glue.model.GetConnectionsResult;
import com.amazonaws.services.glue.model.GetCrawlerMetricsRequest;
import com.amazonaws.services.glue.model.GetCrawlerMetricsResult;
import com.amazonaws.services.glue.model.GetCrawlerRequest;
import com.amazonaws.services.glue.model.GetCrawlerResult;
import com.amazonaws.services.glue.model.GetCrawlersRequest;
import com.amazonaws.services.glue.model.GetCrawlersResult;
import com.amazonaws.services.glue.model.GetDataCatalogEncryptionSettingsRequest;
import com.amazonaws.services.glue.model.GetDataCatalogEncryptionSettingsResult;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetDataflowGraphRequest;
import com.amazonaws.services.glue.model.GetDataflowGraphResult;
import com.amazonaws.services.glue.model.GetDevEndpointRequest;
import com.amazonaws.services.glue.model.GetDevEndpointResult;
import com.amazonaws.services.glue.model.GetDevEndpointsRequest;
import com.amazonaws.services.glue.model.GetDevEndpointsResult;
import com.amazonaws.services.glue.model.GetJobRequest;
import com.amazonaws.services.glue.model.GetJobResult;
import com.amazonaws.services.glue.model.GetJobRunRequest;
import com.amazonaws.services.glue.model.GetJobRunResult;
import com.amazonaws.services.glue.model.GetJobRunsRequest;
import com.amazonaws.services.glue.model.GetJobRunsResult;
import com.amazonaws.services.glue.model.GetJobsRequest;
import com.amazonaws.services.glue.model.GetJobsResult;
import com.amazonaws.services.glue.model.GetMappingRequest;
import com.amazonaws.services.glue.model.GetMappingResult;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetPlanRequest;
import com.amazonaws.services.glue.model.GetPlanResult;
import com.amazonaws.services.glue.model.GetResourcePolicyRequest;
import com.amazonaws.services.glue.model.GetResourcePolicyResult;
import com.amazonaws.services.glue.model.GetSecurityConfigurationRequest;
import com.amazonaws.services.glue.model.GetSecurityConfigurationResult;
import com.amazonaws.services.glue.model.GetSecurityConfigurationsRequest;
import com.amazonaws.services.glue.model.GetSecurityConfigurationsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTableVersionRequest;
import com.amazonaws.services.glue.model.GetTableVersionResult;
import com.amazonaws.services.glue.model.GetTableVersionsRequest;
import com.amazonaws.services.glue.model.GetTableVersionsResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.GetTriggerRequest;
import com.amazonaws.services.glue.model.GetTriggerResult;
import com.amazonaws.services.glue.model.GetTriggersRequest;
import com.amazonaws.services.glue.model.GetTriggersResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsResult;
import com.amazonaws.services.glue.model.GlueEncryptionException;
import com.amazonaws.services.glue.model.ImportCatalogToGlueRequest;
import com.amazonaws.services.glue.model.ImportCatalogToGlueResult;
import com.amazonaws.services.glue.model.InternalServiceException;
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.OperationTimeoutException;
import com.amazonaws.services.glue.model.PutDataCatalogEncryptionSettingsRequest;
import com.amazonaws.services.glue.model.PutDataCatalogEncryptionSettingsResult;
import com.amazonaws.services.glue.model.PutResourcePolicyRequest;
import com.amazonaws.services.glue.model.PutResourcePolicyResult;
import com.amazonaws.services.glue.model.ResetJobBookmarkRequest;
import com.amazonaws.services.glue.model.ResetJobBookmarkResult;
import com.amazonaws.services.glue.model.ResourceNumberLimitExceededException;
import com.amazonaws.services.glue.model.StartCrawlerRequest;
import com.amazonaws.services.glue.model.StartCrawlerResult;
import com.amazonaws.services.glue.model.StartCrawlerScheduleRequest;
import com.amazonaws.services.glue.model.StartCrawlerScheduleResult;
import com.amazonaws.services.glue.model.StartJobRunRequest;
import com.amazonaws.services.glue.model.StartJobRunResult;
import com.amazonaws.services.glue.model.StartTriggerRequest;
import com.amazonaws.services.glue.model.StartTriggerResult;
import com.amazonaws.services.glue.model.StopCrawlerRequest;
import com.amazonaws.services.glue.model.StopCrawlerResult;
import com.amazonaws.services.glue.model.StopCrawlerScheduleRequest;
import com.amazonaws.services.glue.model.StopCrawlerScheduleResult;
import com.amazonaws.services.glue.model.StopTriggerRequest;
import com.amazonaws.services.glue.model.StopTriggerResult;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.UpdateClassifierRequest;
import com.amazonaws.services.glue.model.UpdateClassifierResult;
import com.amazonaws.services.glue.model.UpdateConnectionRequest;
import com.amazonaws.services.glue.model.UpdateConnectionResult;
import com.amazonaws.services.glue.model.UpdateCrawlerRequest;
import com.amazonaws.services.glue.model.UpdateCrawlerResult;
import com.amazonaws.services.glue.model.UpdateCrawlerScheduleRequest;
import com.amazonaws.services.glue.model.UpdateCrawlerScheduleResult;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdateDatabaseResult;
import com.amazonaws.services.glue.model.UpdateDevEndpointRequest;
import com.amazonaws.services.glue.model.UpdateDevEndpointResult;
import com.amazonaws.services.glue.model.UpdateJobRequest;
import com.amazonaws.services.glue.model.UpdateJobResult;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdatePartitionResult;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.UpdateTableResult;
import com.amazonaws.services.glue.model.UpdateTriggerRequest;
import com.amazonaws.services.glue.model.UpdateTriggerResult;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionResult;
import io.prestosql.plugin.hive.metastore.glue.GlueMetastoreApiStats;
import io.prestosql.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.prestosql.spi.connector.ConnectorSession;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.Future;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class GlueIcebergTableOperationsTest
{
    @Mock
    private AWSGlueAsync mockGlueClient;
    @Mock
    private GlueMetastoreStats mockStats;
    @Mock
    private FileIO mockFileIo;
    @Mock
    private ConnectorSession mockSession;

    private GlueIcebergTableOperations glueIcebergTableOperationsUnderTest;

    @BeforeMethod
    public void setUp()
    {
        initMocks(this);
        AWSGlueAsync aWSGlueAsync = new AWSGlueAsync()
        {
            @Override
            public BatchCreatePartitionResult batchCreatePartition(BatchCreatePartitionRequest batchCreatePartitionRequest)
            {
                return null;
            }

            @Override
            public BatchDeleteConnectionResult batchDeleteConnection(BatchDeleteConnectionRequest batchDeleteConnectionRequest)
            {
                return null;
            }

            @Override
            public BatchDeletePartitionResult batchDeletePartition(BatchDeletePartitionRequest batchDeletePartitionRequest)
            {
                return null;
            }

            @Override
            public BatchDeleteTableResult batchDeleteTable(BatchDeleteTableRequest batchDeleteTableRequest)
            {
                return null;
            }

            @Override
            public BatchDeleteTableVersionResult batchDeleteTableVersion(BatchDeleteTableVersionRequest batchDeleteTableVersionRequest)
            {
                return null;
            }

            @Override
            public BatchGetPartitionResult batchGetPartition(BatchGetPartitionRequest batchGetPartitionRequest)
            {
                return null;
            }

            @Override
            public BatchStopJobRunResult batchStopJobRun(BatchStopJobRunRequest batchStopJobRunRequest)
            {
                return null;
            }

            @Override
            public CreateClassifierResult createClassifier(CreateClassifierRequest createClassifierRequest)
            {
                return null;
            }

            @Override
            public CreateConnectionResult createConnection(CreateConnectionRequest createConnectionRequest)
            {
                return null;
            }

            @Override
            public CreateCrawlerResult createCrawler(CreateCrawlerRequest createCrawlerRequest)
            {
                return null;
            }

            @Override
            public CreateDatabaseResult createDatabase(CreateDatabaseRequest createDatabaseRequest)
            {
                return null;
            }

            @Override
            public CreateDevEndpointResult createDevEndpoint(CreateDevEndpointRequest createDevEndpointRequest)
            {
                return null;
            }

            @Override
            public CreateJobResult createJob(CreateJobRequest createJobRequest)
            {
                return null;
            }

            @Override
            public CreatePartitionResult createPartition(CreatePartitionRequest createPartitionRequest)
            {
                return null;
            }

            @Override
            public CreateScriptResult createScript(CreateScriptRequest createScriptRequest)
            {
                return null;
            }

            @Override
            public CreateSecurityConfigurationResult createSecurityConfiguration(CreateSecurityConfigurationRequest createSecurityConfigurationRequest)
            {
                return null;
            }

            @Override
            public CreateTableResult createTable(CreateTableRequest createTableRequest)
            {
                return null;
            }

            @Override
            public CreateTriggerResult createTrigger(CreateTriggerRequest createTriggerRequest)
            {
                return null;
            }

            @Override
            public CreateUserDefinedFunctionResult createUserDefinedFunction(CreateUserDefinedFunctionRequest createUserDefinedFunctionRequest)
            {
                return null;
            }

            @Override
            public DeleteClassifierResult deleteClassifier(DeleteClassifierRequest deleteClassifierRequest)
            {
                return null;
            }

            @Override
            public DeleteConnectionResult deleteConnection(DeleteConnectionRequest deleteConnectionRequest)
            {
                return null;
            }

            @Override
            public DeleteCrawlerResult deleteCrawler(DeleteCrawlerRequest deleteCrawlerRequest)
            {
                return null;
            }

            @Override
            public DeleteDatabaseResult deleteDatabase(DeleteDatabaseRequest deleteDatabaseRequest)
            {
                return null;
            }

            @Override
            public DeleteDevEndpointResult deleteDevEndpoint(DeleteDevEndpointRequest deleteDevEndpointRequest)
            {
                return null;
            }

            @Override
            public DeleteJobResult deleteJob(DeleteJobRequest deleteJobRequest)
            {
                return null;
            }

            @Override
            public DeletePartitionResult deletePartition(DeletePartitionRequest deletePartitionRequest)
            {
                return null;
            }

            @Override
            public DeleteResourcePolicyResult deleteResourcePolicy(DeleteResourcePolicyRequest deleteResourcePolicyRequest)
            {
                return null;
            }

            @Override
            public DeleteSecurityConfigurationResult deleteSecurityConfiguration(DeleteSecurityConfigurationRequest deleteSecurityConfigurationRequest)
            {
                return null;
            }

            @Override
            public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest)
            {
                return null;
            }

            @Override
            public DeleteTableVersionResult deleteTableVersion(DeleteTableVersionRequest deleteTableVersionRequest)
            {
                return null;
            }

            @Override
            public DeleteTriggerResult deleteTrigger(DeleteTriggerRequest deleteTriggerRequest)
            {
                return null;
            }

            @Override
            public DeleteUserDefinedFunctionResult deleteUserDefinedFunction(DeleteUserDefinedFunctionRequest deleteUserDefinedFunctionRequest)
            {
                return null;
            }

            @Override
            public GetCatalogImportStatusResult getCatalogImportStatus(GetCatalogImportStatusRequest getCatalogImportStatusRequest)
            {
                return null;
            }

            @Override
            public GetClassifierResult getClassifier(GetClassifierRequest getClassifierRequest)
            {
                return null;
            }

            @Override
            public GetClassifiersResult getClassifiers(GetClassifiersRequest getClassifiersRequest)
            {
                return null;
            }

            @Override
            public GetConnectionResult getConnection(GetConnectionRequest getConnectionRequest)
            {
                return null;
            }

            @Override
            public GetConnectionsResult getConnections(GetConnectionsRequest getConnectionsRequest)
            {
                return null;
            }

            @Override
            public GetCrawlerResult getCrawler(GetCrawlerRequest getCrawlerRequest)
            {
                return null;
            }

            @Override
            public GetCrawlerMetricsResult getCrawlerMetrics(GetCrawlerMetricsRequest getCrawlerMetricsRequest)
            {
                return null;
            }

            @Override
            public GetCrawlersResult getCrawlers(GetCrawlersRequest getCrawlersRequest)
            {
                return null;
            }

            @Override
            public GetDataCatalogEncryptionSettingsResult getDataCatalogEncryptionSettings(GetDataCatalogEncryptionSettingsRequest getDataCatalogEncryptionSettingsRequest)
            {
                return null;
            }

            @Override
            public GetDatabaseResult getDatabase(GetDatabaseRequest getDatabaseRequest)
            {
                return null;
            }

            @Override
            public GetDatabasesResult getDatabases(GetDatabasesRequest getDatabasesRequest)
            {
                return null;
            }

            @Override
            public GetDataflowGraphResult getDataflowGraph(GetDataflowGraphRequest getDataflowGraphRequest)
            {
                return null;
            }

            @Override
            public GetDevEndpointResult getDevEndpoint(GetDevEndpointRequest getDevEndpointRequest)
            {
                return null;
            }

            @Override
            public GetDevEndpointsResult getDevEndpoints(GetDevEndpointsRequest getDevEndpointsRequest)
            {
                return null;
            }

            @Override
            public GetJobResult getJob(GetJobRequest getJobRequest)
            {
                return null;
            }

            @Override
            public GetJobRunResult getJobRun(GetJobRunRequest getJobRunRequest)
            {
                return null;
            }

            @Override
            public GetJobRunsResult getJobRuns(GetJobRunsRequest getJobRunsRequest)
            {
                return null;
            }

            @Override
            public GetJobsResult getJobs(GetJobsRequest getJobsRequest)
            {
                return null;
            }

            @Override
            public GetMappingResult getMapping(GetMappingRequest getMappingRequest)
            {
                return null;
            }

            @Override
            public GetPartitionResult getPartition(GetPartitionRequest getPartitionRequest)
            {
                return null;
            }

            @Override
            public GetPartitionsResult getPartitions(GetPartitionsRequest getPartitionsRequest)
            {
                return null;
            }

            @Override
            public GetPlanResult getPlan(GetPlanRequest getPlanRequest)
            {
                return null;
            }

            @Override
            public GetResourcePolicyResult getResourcePolicy(GetResourcePolicyRequest getResourcePolicyRequest)
            {
                return null;
            }

            @Override
            public GetSecurityConfigurationResult getSecurityConfiguration(GetSecurityConfigurationRequest getSecurityConfigurationRequest)
            {
                return null;
            }

            @Override
            public GetSecurityConfigurationsResult getSecurityConfigurations(GetSecurityConfigurationsRequest getSecurityConfigurationsRequest)
            {
                return null;
            }

            @Override
            public GetTableResult getTable(GetTableRequest getTableRequest)
            {
                return null;
            }

            @Override
            public GetTableVersionResult getTableVersion(GetTableVersionRequest getTableVersionRequest)
            {
                return null;
            }

            @Override
            public GetTableVersionsResult getTableVersions(GetTableVersionsRequest getTableVersionsRequest)
            {
                return null;
            }

            @Override
            public GetTablesResult getTables(GetTablesRequest getTablesRequest)
            {
                return null;
            }

            @Override
            public GetTriggerResult getTrigger(GetTriggerRequest getTriggerRequest)
            {
                return null;
            }

            @Override
            public GetTriggersResult getTriggers(GetTriggersRequest getTriggersRequest)
            {
                return null;
            }

            @Override
            public GetUserDefinedFunctionResult getUserDefinedFunction(GetUserDefinedFunctionRequest getUserDefinedFunctionRequest)
            {
                return null;
            }

            @Override
            public GetUserDefinedFunctionsResult getUserDefinedFunctions(GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest)
            {
                return null;
            }

            @Override
            public ImportCatalogToGlueResult importCatalogToGlue(ImportCatalogToGlueRequest importCatalogToGlueRequest)
            {
                return null;
            }

            @Override
            public PutDataCatalogEncryptionSettingsResult putDataCatalogEncryptionSettings(PutDataCatalogEncryptionSettingsRequest putDataCatalogEncryptionSettingsRequest)
            {
                return null;
            }

            @Override
            public PutResourcePolicyResult putResourcePolicy(PutResourcePolicyRequest putResourcePolicyRequest)
            {
                return null;
            }

            @Override
            public ResetJobBookmarkResult resetJobBookmark(ResetJobBookmarkRequest resetJobBookmarkRequest)
            {
                return null;
            }

            @Override
            public StartCrawlerResult startCrawler(StartCrawlerRequest startCrawlerRequest)
            {
                return null;
            }

            @Override
            public StartCrawlerScheduleResult startCrawlerSchedule(StartCrawlerScheduleRequest startCrawlerScheduleRequest)
            {
                return null;
            }

            @Override
            public StartJobRunResult startJobRun(StartJobRunRequest startJobRunRequest)
            {
                return null;
            }

            @Override
            public StartTriggerResult startTrigger(StartTriggerRequest startTriggerRequest)
            {
                return null;
            }

            @Override
            public StopCrawlerResult stopCrawler(StopCrawlerRequest stopCrawlerRequest)
            {
                return null;
            }

            @Override
            public StopCrawlerScheduleResult stopCrawlerSchedule(StopCrawlerScheduleRequest stopCrawlerScheduleRequest)
            {
                return null;
            }

            @Override
            public StopTriggerResult stopTrigger(StopTriggerRequest stopTriggerRequest)
            {
                return null;
            }

            @Override
            public UpdateClassifierResult updateClassifier(UpdateClassifierRequest updateClassifierRequest)
            {
                return null;
            }

            @Override
            public UpdateConnectionResult updateConnection(UpdateConnectionRequest updateConnectionRequest)
            {
                return null;
            }

            @Override
            public UpdateCrawlerResult updateCrawler(UpdateCrawlerRequest updateCrawlerRequest)
            {
                return null;
            }

            @Override
            public UpdateCrawlerScheduleResult updateCrawlerSchedule(UpdateCrawlerScheduleRequest updateCrawlerScheduleRequest)
            {
                return null;
            }

            @Override
            public UpdateDatabaseResult updateDatabase(UpdateDatabaseRequest updateDatabaseRequest)
            {
                return null;
            }

            @Override
            public UpdateDevEndpointResult updateDevEndpoint(UpdateDevEndpointRequest updateDevEndpointRequest)
            {
                return null;
            }

            @Override
            public UpdateJobResult updateJob(UpdateJobRequest updateJobRequest)
            {
                return null;
            }

            @Override
            public UpdatePartitionResult updatePartition(UpdatePartitionRequest updatePartitionRequest)
            {
                return null;
            }

            @Override
            public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest)
            {
                return null;
            }

            @Override
            public UpdateTriggerResult updateTrigger(UpdateTriggerRequest updateTriggerRequest)
            {
                return null;
            }

            @Override
            public UpdateUserDefinedFunctionResult updateUserDefinedFunction(UpdateUserDefinedFunctionRequest updateUserDefinedFunctionRequest)
            {
                return null;
            }

            @Override
            public void shutdown()
            {
            }

            @Override
            public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request)
            {
                return null;
            }

            @Override
            public Future<BatchCreatePartitionResult> batchCreatePartitionAsync(BatchCreatePartitionRequest batchCreatePartitionRequest)
            {
                return null;
            }

            @Override
            public Future<BatchCreatePartitionResult> batchCreatePartitionAsync(BatchCreatePartitionRequest batchCreatePartitionRequest, AsyncHandler<BatchCreatePartitionRequest, BatchCreatePartitionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<BatchDeleteConnectionResult> batchDeleteConnectionAsync(BatchDeleteConnectionRequest batchDeleteConnectionRequest)
            {
                return null;
            }

            @Override
            public Future<BatchDeleteConnectionResult> batchDeleteConnectionAsync(BatchDeleteConnectionRequest batchDeleteConnectionRequest, AsyncHandler<BatchDeleteConnectionRequest, BatchDeleteConnectionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<BatchDeletePartitionResult> batchDeletePartitionAsync(BatchDeletePartitionRequest batchDeletePartitionRequest)
            {
                return null;
            }

            @Override
            public Future<BatchDeletePartitionResult> batchDeletePartitionAsync(BatchDeletePartitionRequest batchDeletePartitionRequest, AsyncHandler<BatchDeletePartitionRequest, BatchDeletePartitionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<BatchDeleteTableResult> batchDeleteTableAsync(BatchDeleteTableRequest batchDeleteTableRequest)
            {
                return null;
            }

            @Override
            public Future<BatchDeleteTableResult> batchDeleteTableAsync(BatchDeleteTableRequest batchDeleteTableRequest, AsyncHandler<BatchDeleteTableRequest, BatchDeleteTableResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<BatchDeleteTableVersionResult> batchDeleteTableVersionAsync(BatchDeleteTableVersionRequest batchDeleteTableVersionRequest)
            {
                return null;
            }

            @Override
            public Future<BatchDeleteTableVersionResult> batchDeleteTableVersionAsync(BatchDeleteTableVersionRequest batchDeleteTableVersionRequest, AsyncHandler<BatchDeleteTableVersionRequest, BatchDeleteTableVersionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<BatchGetPartitionResult> batchGetPartitionAsync(BatchGetPartitionRequest batchGetPartitionRequest)
            {
                return null;
            }

            @Override
            public Future<BatchGetPartitionResult> batchGetPartitionAsync(BatchGetPartitionRequest batchGetPartitionRequest, AsyncHandler<BatchGetPartitionRequest, BatchGetPartitionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<BatchStopJobRunResult> batchStopJobRunAsync(BatchStopJobRunRequest batchStopJobRunRequest)
            {
                return null;
            }

            @Override
            public Future<BatchStopJobRunResult> batchStopJobRunAsync(BatchStopJobRunRequest batchStopJobRunRequest, AsyncHandler<BatchStopJobRunRequest, BatchStopJobRunResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<CreateClassifierResult> createClassifierAsync(CreateClassifierRequest createClassifierRequest)
            {
                return null;
            }

            @Override
            public Future<CreateClassifierResult> createClassifierAsync(CreateClassifierRequest createClassifierRequest, AsyncHandler<CreateClassifierRequest, CreateClassifierResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<CreateConnectionResult> createConnectionAsync(CreateConnectionRequest createConnectionRequest)
            {
                return null;
            }

            @Override
            public Future<CreateConnectionResult> createConnectionAsync(CreateConnectionRequest createConnectionRequest, AsyncHandler<CreateConnectionRequest, CreateConnectionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<CreateCrawlerResult> createCrawlerAsync(CreateCrawlerRequest createCrawlerRequest)
            {
                return null;
            }

            @Override
            public Future<CreateCrawlerResult> createCrawlerAsync(CreateCrawlerRequest createCrawlerRequest, AsyncHandler<CreateCrawlerRequest, CreateCrawlerResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<CreateDatabaseResult> createDatabaseAsync(CreateDatabaseRequest createDatabaseRequest)
            {
                return null;
            }

            @Override
            public Future<CreateDatabaseResult> createDatabaseAsync(CreateDatabaseRequest createDatabaseRequest, AsyncHandler<CreateDatabaseRequest, CreateDatabaseResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<CreateDevEndpointResult> createDevEndpointAsync(CreateDevEndpointRequest createDevEndpointRequest)
            {
                return null;
            }

            @Override
            public Future<CreateDevEndpointResult> createDevEndpointAsync(CreateDevEndpointRequest createDevEndpointRequest, AsyncHandler<CreateDevEndpointRequest, CreateDevEndpointResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<CreateJobResult> createJobAsync(CreateJobRequest createJobRequest)
            {
                return null;
            }

            @Override
            public Future<CreateJobResult> createJobAsync(CreateJobRequest createJobRequest, AsyncHandler<CreateJobRequest, CreateJobResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<CreatePartitionResult> createPartitionAsync(CreatePartitionRequest createPartitionRequest)
            {
                return null;
            }

            @Override
            public Future<CreatePartitionResult> createPartitionAsync(CreatePartitionRequest createPartitionRequest, AsyncHandler<CreatePartitionRequest, CreatePartitionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<CreateScriptResult> createScriptAsync(CreateScriptRequest createScriptRequest)
            {
                return null;
            }

            @Override
            public Future<CreateScriptResult> createScriptAsync(CreateScriptRequest createScriptRequest, AsyncHandler<CreateScriptRequest, CreateScriptResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<CreateSecurityConfigurationResult> createSecurityConfigurationAsync(CreateSecurityConfigurationRequest createSecurityConfigurationRequest)
            {
                return null;
            }

            @Override
            public Future<CreateSecurityConfigurationResult> createSecurityConfigurationAsync(CreateSecurityConfigurationRequest createSecurityConfigurationRequest, AsyncHandler<CreateSecurityConfigurationRequest, CreateSecurityConfigurationResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<CreateTableResult> createTableAsync(CreateTableRequest createTableRequest)
            {
                return null;
            }

            @Override
            public Future<CreateTableResult> createTableAsync(CreateTableRequest createTableRequest, AsyncHandler<CreateTableRequest, CreateTableResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<CreateTriggerResult> createTriggerAsync(CreateTriggerRequest createTriggerRequest)
            {
                return null;
            }

            @Override
            public Future<CreateTriggerResult> createTriggerAsync(CreateTriggerRequest createTriggerRequest, AsyncHandler<CreateTriggerRequest, CreateTriggerResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<CreateUserDefinedFunctionResult> createUserDefinedFunctionAsync(CreateUserDefinedFunctionRequest createUserDefinedFunctionRequest)
            {
                return null;
            }

            @Override
            public Future<CreateUserDefinedFunctionResult> createUserDefinedFunctionAsync(CreateUserDefinedFunctionRequest createUserDefinedFunctionRequest, AsyncHandler<CreateUserDefinedFunctionRequest, CreateUserDefinedFunctionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<DeleteClassifierResult> deleteClassifierAsync(DeleteClassifierRequest deleteClassifierRequest)
            {
                return null;
            }

            @Override
            public Future<DeleteClassifierResult> deleteClassifierAsync(DeleteClassifierRequest deleteClassifierRequest, AsyncHandler<DeleteClassifierRequest, DeleteClassifierResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<DeleteConnectionResult> deleteConnectionAsync(DeleteConnectionRequest deleteConnectionRequest)
            {
                return null;
            }

            @Override
            public Future<DeleteConnectionResult> deleteConnectionAsync(DeleteConnectionRequest deleteConnectionRequest, AsyncHandler<DeleteConnectionRequest, DeleteConnectionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<DeleteCrawlerResult> deleteCrawlerAsync(DeleteCrawlerRequest deleteCrawlerRequest)
            {
                return null;
            }

            @Override
            public Future<DeleteCrawlerResult> deleteCrawlerAsync(DeleteCrawlerRequest deleteCrawlerRequest, AsyncHandler<DeleteCrawlerRequest, DeleteCrawlerResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<DeleteDatabaseResult> deleteDatabaseAsync(DeleteDatabaseRequest deleteDatabaseRequest)
            {
                return null;
            }

            @Override
            public Future<DeleteDatabaseResult> deleteDatabaseAsync(DeleteDatabaseRequest deleteDatabaseRequest, AsyncHandler<DeleteDatabaseRequest, DeleteDatabaseResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<DeleteDevEndpointResult> deleteDevEndpointAsync(DeleteDevEndpointRequest deleteDevEndpointRequest)
            {
                return null;
            }

            @Override
            public Future<DeleteDevEndpointResult> deleteDevEndpointAsync(DeleteDevEndpointRequest deleteDevEndpointRequest, AsyncHandler<DeleteDevEndpointRequest, DeleteDevEndpointResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<DeleteJobResult> deleteJobAsync(DeleteJobRequest deleteJobRequest)
            {
                return null;
            }

            @Override
            public Future<DeleteJobResult> deleteJobAsync(DeleteJobRequest deleteJobRequest, AsyncHandler<DeleteJobRequest, DeleteJobResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<DeletePartitionResult> deletePartitionAsync(DeletePartitionRequest deletePartitionRequest)
            {
                return null;
            }

            @Override
            public Future<DeletePartitionResult> deletePartitionAsync(DeletePartitionRequest deletePartitionRequest, AsyncHandler<DeletePartitionRequest, DeletePartitionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<DeleteResourcePolicyResult> deleteResourcePolicyAsync(DeleteResourcePolicyRequest deleteResourcePolicyRequest)
            {
                return null;
            }

            @Override
            public Future<DeleteResourcePolicyResult> deleteResourcePolicyAsync(DeleteResourcePolicyRequest deleteResourcePolicyRequest, AsyncHandler<DeleteResourcePolicyRequest, DeleteResourcePolicyResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<DeleteSecurityConfigurationResult> deleteSecurityConfigurationAsync(DeleteSecurityConfigurationRequest deleteSecurityConfigurationRequest)
            {
                return null;
            }

            @Override
            public Future<DeleteSecurityConfigurationResult> deleteSecurityConfigurationAsync(DeleteSecurityConfigurationRequest deleteSecurityConfigurationRequest, AsyncHandler<DeleteSecurityConfigurationRequest, DeleteSecurityConfigurationResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<DeleteTableResult> deleteTableAsync(DeleteTableRequest deleteTableRequest)
            {
                return null;
            }

            @Override
            public Future<DeleteTableResult> deleteTableAsync(DeleteTableRequest deleteTableRequest, AsyncHandler<DeleteTableRequest, DeleteTableResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<DeleteTableVersionResult> deleteTableVersionAsync(DeleteTableVersionRequest deleteTableVersionRequest)
            {
                return null;
            }

            @Override
            public Future<DeleteTableVersionResult> deleteTableVersionAsync(DeleteTableVersionRequest deleteTableVersionRequest, AsyncHandler<DeleteTableVersionRequest, DeleteTableVersionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<DeleteTriggerResult> deleteTriggerAsync(DeleteTriggerRequest deleteTriggerRequest)
            {
                return null;
            }

            @Override
            public Future<DeleteTriggerResult> deleteTriggerAsync(DeleteTriggerRequest deleteTriggerRequest, AsyncHandler<DeleteTriggerRequest, DeleteTriggerResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<DeleteUserDefinedFunctionResult> deleteUserDefinedFunctionAsync(DeleteUserDefinedFunctionRequest deleteUserDefinedFunctionRequest)
            {
                return null;
            }

            @Override
            public Future<DeleteUserDefinedFunctionResult> deleteUserDefinedFunctionAsync(DeleteUserDefinedFunctionRequest deleteUserDefinedFunctionRequest, AsyncHandler<DeleteUserDefinedFunctionRequest, DeleteUserDefinedFunctionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetCatalogImportStatusResult> getCatalogImportStatusAsync(GetCatalogImportStatusRequest getCatalogImportStatusRequest)
            {
                return null;
            }

            @Override
            public Future<GetCatalogImportStatusResult> getCatalogImportStatusAsync(GetCatalogImportStatusRequest getCatalogImportStatusRequest, AsyncHandler<GetCatalogImportStatusRequest, GetCatalogImportStatusResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetClassifierResult> getClassifierAsync(GetClassifierRequest getClassifierRequest)
            {
                return null;
            }

            @Override
            public Future<GetClassifierResult> getClassifierAsync(GetClassifierRequest getClassifierRequest, AsyncHandler<GetClassifierRequest, GetClassifierResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetClassifiersResult> getClassifiersAsync(GetClassifiersRequest getClassifiersRequest)
            {
                return null;
            }

            @Override
            public Future<GetClassifiersResult> getClassifiersAsync(GetClassifiersRequest getClassifiersRequest, AsyncHandler<GetClassifiersRequest, GetClassifiersResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetConnectionResult> getConnectionAsync(GetConnectionRequest getConnectionRequest)
            {
                return null;
            }

            @Override
            public Future<GetConnectionResult> getConnectionAsync(GetConnectionRequest getConnectionRequest, AsyncHandler<GetConnectionRequest, GetConnectionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetConnectionsResult> getConnectionsAsync(GetConnectionsRequest getConnectionsRequest)
            {
                return null;
            }

            @Override
            public Future<GetConnectionsResult> getConnectionsAsync(GetConnectionsRequest getConnectionsRequest, AsyncHandler<GetConnectionsRequest, GetConnectionsResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetCrawlerResult> getCrawlerAsync(GetCrawlerRequest getCrawlerRequest)
            {
                return null;
            }

            @Override
            public Future<GetCrawlerResult> getCrawlerAsync(GetCrawlerRequest getCrawlerRequest, AsyncHandler<GetCrawlerRequest, GetCrawlerResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetCrawlerMetricsResult> getCrawlerMetricsAsync(GetCrawlerMetricsRequest getCrawlerMetricsRequest)
            {
                return null;
            }

            @Override
            public Future<GetCrawlerMetricsResult> getCrawlerMetricsAsync(GetCrawlerMetricsRequest getCrawlerMetricsRequest, AsyncHandler<GetCrawlerMetricsRequest, GetCrawlerMetricsResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetCrawlersResult> getCrawlersAsync(GetCrawlersRequest getCrawlersRequest)
            {
                return null;
            }

            @Override
            public Future<GetCrawlersResult> getCrawlersAsync(GetCrawlersRequest getCrawlersRequest, AsyncHandler<GetCrawlersRequest, GetCrawlersResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetDataCatalogEncryptionSettingsResult> getDataCatalogEncryptionSettingsAsync(GetDataCatalogEncryptionSettingsRequest getDataCatalogEncryptionSettingsRequest)
            {
                return null;
            }

            @Override
            public Future<GetDataCatalogEncryptionSettingsResult> getDataCatalogEncryptionSettingsAsync(GetDataCatalogEncryptionSettingsRequest getDataCatalogEncryptionSettingsRequest, AsyncHandler<GetDataCatalogEncryptionSettingsRequest, GetDataCatalogEncryptionSettingsResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetDatabaseResult> getDatabaseAsync(GetDatabaseRequest getDatabaseRequest)
            {
                return null;
            }

            @Override
            public Future<GetDatabaseResult> getDatabaseAsync(GetDatabaseRequest getDatabaseRequest, AsyncHandler<GetDatabaseRequest, GetDatabaseResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetDatabasesResult> getDatabasesAsync(GetDatabasesRequest getDatabasesRequest)
            {
                return null;
            }

            @Override
            public Future<GetDatabasesResult> getDatabasesAsync(GetDatabasesRequest getDatabasesRequest, AsyncHandler<GetDatabasesRequest, GetDatabasesResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetDataflowGraphResult> getDataflowGraphAsync(GetDataflowGraphRequest getDataflowGraphRequest)
            {
                return null;
            }

            @Override
            public Future<GetDataflowGraphResult> getDataflowGraphAsync(GetDataflowGraphRequest getDataflowGraphRequest, AsyncHandler<GetDataflowGraphRequest, GetDataflowGraphResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetDevEndpointResult> getDevEndpointAsync(GetDevEndpointRequest getDevEndpointRequest)
            {
                return null;
            }

            @Override
            public Future<GetDevEndpointResult> getDevEndpointAsync(GetDevEndpointRequest getDevEndpointRequest, AsyncHandler<GetDevEndpointRequest, GetDevEndpointResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetDevEndpointsResult> getDevEndpointsAsync(GetDevEndpointsRequest getDevEndpointsRequest)
            {
                return null;
            }

            @Override
            public Future<GetDevEndpointsResult> getDevEndpointsAsync(GetDevEndpointsRequest getDevEndpointsRequest, AsyncHandler<GetDevEndpointsRequest, GetDevEndpointsResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetJobResult> getJobAsync(GetJobRequest getJobRequest)
            {
                return null;
            }

            @Override
            public Future<GetJobResult> getJobAsync(GetJobRequest getJobRequest, AsyncHandler<GetJobRequest, GetJobResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetJobRunResult> getJobRunAsync(GetJobRunRequest getJobRunRequest)
            {
                return null;
            }

            @Override
            public Future<GetJobRunResult> getJobRunAsync(GetJobRunRequest getJobRunRequest, AsyncHandler<GetJobRunRequest, GetJobRunResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetJobRunsResult> getJobRunsAsync(GetJobRunsRequest getJobRunsRequest)
            {
                return null;
            }

            @Override
            public Future<GetJobRunsResult> getJobRunsAsync(GetJobRunsRequest getJobRunsRequest, AsyncHandler<GetJobRunsRequest, GetJobRunsResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetJobsResult> getJobsAsync(GetJobsRequest getJobsRequest)
            {
                return null;
            }

            @Override
            public Future<GetJobsResult> getJobsAsync(GetJobsRequest getJobsRequest, AsyncHandler<GetJobsRequest, GetJobsResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetMappingResult> getMappingAsync(GetMappingRequest getMappingRequest)
            {
                return null;
            }

            @Override
            public Future<GetMappingResult> getMappingAsync(GetMappingRequest getMappingRequest, AsyncHandler<GetMappingRequest, GetMappingResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetPartitionResult> getPartitionAsync(GetPartitionRequest getPartitionRequest)
            {
                return null;
            }

            @Override
            public Future<GetPartitionResult> getPartitionAsync(GetPartitionRequest getPartitionRequest, AsyncHandler<GetPartitionRequest, GetPartitionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetPartitionsResult> getPartitionsAsync(GetPartitionsRequest getPartitionsRequest)
            {
                return null;
            }

            @Override
            public Future<GetPartitionsResult> getPartitionsAsync(GetPartitionsRequest getPartitionsRequest, AsyncHandler<GetPartitionsRequest, GetPartitionsResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetPlanResult> getPlanAsync(GetPlanRequest getPlanRequest)
            {
                return null;
            }

            @Override
            public Future<GetPlanResult> getPlanAsync(GetPlanRequest getPlanRequest, AsyncHandler<GetPlanRequest, GetPlanResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetResourcePolicyResult> getResourcePolicyAsync(GetResourcePolicyRequest getResourcePolicyRequest)
            {
                return null;
            }

            @Override
            public Future<GetResourcePolicyResult> getResourcePolicyAsync(GetResourcePolicyRequest getResourcePolicyRequest, AsyncHandler<GetResourcePolicyRequest, GetResourcePolicyResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetSecurityConfigurationResult> getSecurityConfigurationAsync(GetSecurityConfigurationRequest getSecurityConfigurationRequest)
            {
                return null;
            }

            @Override
            public Future<GetSecurityConfigurationResult> getSecurityConfigurationAsync(GetSecurityConfigurationRequest getSecurityConfigurationRequest, AsyncHandler<GetSecurityConfigurationRequest, GetSecurityConfigurationResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetSecurityConfigurationsResult> getSecurityConfigurationsAsync(GetSecurityConfigurationsRequest getSecurityConfigurationsRequest)
            {
                return null;
            }

            @Override
            public Future<GetSecurityConfigurationsResult> getSecurityConfigurationsAsync(GetSecurityConfigurationsRequest getSecurityConfigurationsRequest, AsyncHandler<GetSecurityConfigurationsRequest, GetSecurityConfigurationsResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetTableResult> getTableAsync(GetTableRequest getTableRequest)
            {
                return null;
            }

            @Override
            public Future<GetTableResult> getTableAsync(GetTableRequest getTableRequest, AsyncHandler<GetTableRequest, GetTableResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetTableVersionResult> getTableVersionAsync(GetTableVersionRequest getTableVersionRequest)
            {
                return null;
            }

            @Override
            public Future<GetTableVersionResult> getTableVersionAsync(GetTableVersionRequest getTableVersionRequest, AsyncHandler<GetTableVersionRequest, GetTableVersionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetTableVersionsResult> getTableVersionsAsync(GetTableVersionsRequest getTableVersionsRequest)
            {
                return null;
            }

            @Override
            public Future<GetTableVersionsResult> getTableVersionsAsync(GetTableVersionsRequest getTableVersionsRequest, AsyncHandler<GetTableVersionsRequest, GetTableVersionsResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetTablesResult> getTablesAsync(GetTablesRequest getTablesRequest)
            {
                return null;
            }

            @Override
            public Future<GetTablesResult> getTablesAsync(GetTablesRequest getTablesRequest, AsyncHandler<GetTablesRequest, GetTablesResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetTriggerResult> getTriggerAsync(GetTriggerRequest getTriggerRequest)
            {
                return null;
            }

            @Override
            public Future<GetTriggerResult> getTriggerAsync(GetTriggerRequest getTriggerRequest, AsyncHandler<GetTriggerRequest, GetTriggerResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetTriggersResult> getTriggersAsync(GetTriggersRequest getTriggersRequest)
            {
                return null;
            }

            @Override
            public Future<GetTriggersResult> getTriggersAsync(GetTriggersRequest getTriggersRequest, AsyncHandler<GetTriggersRequest, GetTriggersResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetUserDefinedFunctionResult> getUserDefinedFunctionAsync(GetUserDefinedFunctionRequest getUserDefinedFunctionRequest)
            {
                return null;
            }

            @Override
            public Future<GetUserDefinedFunctionResult> getUserDefinedFunctionAsync(GetUserDefinedFunctionRequest getUserDefinedFunctionRequest, AsyncHandler<GetUserDefinedFunctionRequest, GetUserDefinedFunctionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<GetUserDefinedFunctionsResult> getUserDefinedFunctionsAsync(GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest)
            {
                return null;
            }

            @Override
            public Future<GetUserDefinedFunctionsResult> getUserDefinedFunctionsAsync(GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest, AsyncHandler<GetUserDefinedFunctionsRequest, GetUserDefinedFunctionsResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<ImportCatalogToGlueResult> importCatalogToGlueAsync(ImportCatalogToGlueRequest importCatalogToGlueRequest)
            {
                return null;
            }

            @Override
            public Future<ImportCatalogToGlueResult> importCatalogToGlueAsync(ImportCatalogToGlueRequest importCatalogToGlueRequest, AsyncHandler<ImportCatalogToGlueRequest, ImportCatalogToGlueResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<PutDataCatalogEncryptionSettingsResult> putDataCatalogEncryptionSettingsAsync(PutDataCatalogEncryptionSettingsRequest putDataCatalogEncryptionSettingsRequest)
            {
                return null;
            }

            @Override
            public Future<PutDataCatalogEncryptionSettingsResult> putDataCatalogEncryptionSettingsAsync(PutDataCatalogEncryptionSettingsRequest putDataCatalogEncryptionSettingsRequest, AsyncHandler<PutDataCatalogEncryptionSettingsRequest, PutDataCatalogEncryptionSettingsResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<PutResourcePolicyResult> putResourcePolicyAsync(PutResourcePolicyRequest putResourcePolicyRequest)
            {
                return null;
            }

            @Override
            public Future<PutResourcePolicyResult> putResourcePolicyAsync(PutResourcePolicyRequest putResourcePolicyRequest, AsyncHandler<PutResourcePolicyRequest, PutResourcePolicyResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<ResetJobBookmarkResult> resetJobBookmarkAsync(ResetJobBookmarkRequest resetJobBookmarkRequest)
            {
                return null;
            }

            @Override
            public Future<ResetJobBookmarkResult> resetJobBookmarkAsync(ResetJobBookmarkRequest resetJobBookmarkRequest, AsyncHandler<ResetJobBookmarkRequest, ResetJobBookmarkResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<StartCrawlerResult> startCrawlerAsync(StartCrawlerRequest startCrawlerRequest)
            {
                return null;
            }

            @Override
            public Future<StartCrawlerResult> startCrawlerAsync(StartCrawlerRequest startCrawlerRequest, AsyncHandler<StartCrawlerRequest, StartCrawlerResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<StartCrawlerScheduleResult> startCrawlerScheduleAsync(StartCrawlerScheduleRequest startCrawlerScheduleRequest)
            {
                return null;
            }

            @Override
            public Future<StartCrawlerScheduleResult> startCrawlerScheduleAsync(StartCrawlerScheduleRequest startCrawlerScheduleRequest, AsyncHandler<StartCrawlerScheduleRequest, StartCrawlerScheduleResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<StartJobRunResult> startJobRunAsync(StartJobRunRequest startJobRunRequest)
            {
                return null;
            }

            @Override
            public Future<StartJobRunResult> startJobRunAsync(StartJobRunRequest startJobRunRequest, AsyncHandler<StartJobRunRequest, StartJobRunResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<StartTriggerResult> startTriggerAsync(StartTriggerRequest startTriggerRequest)
            {
                return null;
            }

            @Override
            public Future<StartTriggerResult> startTriggerAsync(StartTriggerRequest startTriggerRequest, AsyncHandler<StartTriggerRequest, StartTriggerResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<StopCrawlerResult> stopCrawlerAsync(StopCrawlerRequest stopCrawlerRequest)
            {
                return null;
            }

            @Override
            public Future<StopCrawlerResult> stopCrawlerAsync(StopCrawlerRequest stopCrawlerRequest, AsyncHandler<StopCrawlerRequest, StopCrawlerResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<StopCrawlerScheduleResult> stopCrawlerScheduleAsync(StopCrawlerScheduleRequest stopCrawlerScheduleRequest)
            {
                return null;
            }

            @Override
            public Future<StopCrawlerScheduleResult> stopCrawlerScheduleAsync(StopCrawlerScheduleRequest stopCrawlerScheduleRequest, AsyncHandler<StopCrawlerScheduleRequest, StopCrawlerScheduleResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<StopTriggerResult> stopTriggerAsync(StopTriggerRequest stopTriggerRequest)
            {
                return null;
            }

            @Override
            public Future<StopTriggerResult> stopTriggerAsync(StopTriggerRequest stopTriggerRequest, AsyncHandler<StopTriggerRequest, StopTriggerResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<UpdateClassifierResult> updateClassifierAsync(UpdateClassifierRequest updateClassifierRequest)
            {
                return null;
            }

            @Override
            public Future<UpdateClassifierResult> updateClassifierAsync(UpdateClassifierRequest updateClassifierRequest, AsyncHandler<UpdateClassifierRequest, UpdateClassifierResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<UpdateConnectionResult> updateConnectionAsync(UpdateConnectionRequest updateConnectionRequest)
            {
                return null;
            }

            @Override
            public Future<UpdateConnectionResult> updateConnectionAsync(UpdateConnectionRequest updateConnectionRequest, AsyncHandler<UpdateConnectionRequest, UpdateConnectionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<UpdateCrawlerResult> updateCrawlerAsync(UpdateCrawlerRequest updateCrawlerRequest)
            {
                return null;
            }

            @Override
            public Future<UpdateCrawlerResult> updateCrawlerAsync(UpdateCrawlerRequest updateCrawlerRequest, AsyncHandler<UpdateCrawlerRequest, UpdateCrawlerResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<UpdateCrawlerScheduleResult> updateCrawlerScheduleAsync(UpdateCrawlerScheduleRequest updateCrawlerScheduleRequest)
            {
                return null;
            }

            @Override
            public Future<UpdateCrawlerScheduleResult> updateCrawlerScheduleAsync(UpdateCrawlerScheduleRequest updateCrawlerScheduleRequest, AsyncHandler<UpdateCrawlerScheduleRequest, UpdateCrawlerScheduleResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<UpdateDatabaseResult> updateDatabaseAsync(UpdateDatabaseRequest updateDatabaseRequest)
            {
                return null;
            }

            @Override
            public Future<UpdateDatabaseResult> updateDatabaseAsync(UpdateDatabaseRequest updateDatabaseRequest, AsyncHandler<UpdateDatabaseRequest, UpdateDatabaseResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<UpdateDevEndpointResult> updateDevEndpointAsync(UpdateDevEndpointRequest updateDevEndpointRequest)
            {
                return null;
            }

            @Override
            public Future<UpdateDevEndpointResult> updateDevEndpointAsync(UpdateDevEndpointRequest updateDevEndpointRequest, AsyncHandler<UpdateDevEndpointRequest, UpdateDevEndpointResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<UpdateJobResult> updateJobAsync(UpdateJobRequest updateJobRequest)
            {
                return null;
            }

            @Override
            public Future<UpdateJobResult> updateJobAsync(UpdateJobRequest updateJobRequest, AsyncHandler<UpdateJobRequest, UpdateJobResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<UpdatePartitionResult> updatePartitionAsync(UpdatePartitionRequest updatePartitionRequest)
            {
                return null;
            }

            @Override
            public Future<UpdatePartitionResult> updatePartitionAsync(UpdatePartitionRequest updatePartitionRequest, AsyncHandler<UpdatePartitionRequest, UpdatePartitionResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<UpdateTableResult> updateTableAsync(UpdateTableRequest updateTableRequest)
            {
                return null;
            }

            @Override
            public Future<UpdateTableResult> updateTableAsync(UpdateTableRequest updateTableRequest, AsyncHandler<UpdateTableRequest, UpdateTableResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<UpdateTriggerResult> updateTriggerAsync(UpdateTriggerRequest updateTriggerRequest)
            {
                return null;
            }

            @Override
            public Future<UpdateTriggerResult> updateTriggerAsync(UpdateTriggerRequest updateTriggerRequest, AsyncHandler<UpdateTriggerRequest, UpdateTriggerResult> asyncHandler)
            {
                return null;
            }

            @Override
            public Future<UpdateUserDefinedFunctionResult> updateUserDefinedFunctionAsync(UpdateUserDefinedFunctionRequest updateUserDefinedFunctionRequest)
            {
                return null;
            }

            @Override
            public Future<UpdateUserDefinedFunctionResult> updateUserDefinedFunctionAsync(UpdateUserDefinedFunctionRequest updateUserDefinedFunctionRequest, AsyncHandler<UpdateUserDefinedFunctionRequest, UpdateUserDefinedFunctionResult> asyncHandler)
            {
                return null;
            }
        };
        glueIcebergTableOperationsUnderTest = new GlueIcebergTableOperations(aWSGlueAsync, new GlueMetastoreStats(), mockFileIo,
                mockSession, "database", "table",
                Optional.of("value"), Optional.of("value"));
    }

    @Test
    public void testGetRefreshedLocation()
    {
        // Setup
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("name");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        column.setName("name");
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        // Run the test
        final String result = glueIcebergTableOperationsUnderTest.getRefreshedLocation();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testGetRefreshedLocation_AWSGlueAsyncThrowsEntityNotFoundException()
    {
        // Setup
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(EntityNotFoundException.class);

        // Run the test
        final String result = glueIcebergTableOperationsUnderTest.getRefreshedLocation();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testGetRefreshedLocation_AWSGlueAsyncThrowsInvalidInputException()
    {
        // Setup
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InvalidInputException.class);

        // Run the test
        final String result = glueIcebergTableOperationsUnderTest.getRefreshedLocation();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testGetRefreshedLocation_AWSGlueAsyncThrowsInternalServiceException()
    {
        // Setup
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InternalServiceException.class);

        // Run the test
        final String result = glueIcebergTableOperationsUnderTest.getRefreshedLocation();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testGetRefreshedLocation_AWSGlueAsyncThrowsOperationTimeoutException()
    {
        // Setup
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(OperationTimeoutException.class);

        // Run the test
        final String result = glueIcebergTableOperationsUnderTest.getRefreshedLocation();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testGetRefreshedLocation_AWSGlueAsyncThrowsGlueEncryptionException()
    {
        // Setup
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(GlueEncryptionException.class);

        // Run the test
        final String result = glueIcebergTableOperationsUnderTest.getRefreshedLocation();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testCommitNewTable()
    {
        // Setup
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenReturn(new CreateTableResult());

        // Run the test
        glueIcebergTableOperationsUnderTest.commitNewTable(metadata);

        // Verify the results
    }

    @Test
    public void testCommitNewTable_AWSGlueAsyncThrowsAlreadyExistsException()
    {
        // Setup
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(AlreadyExistsException.class);

        // Run the test
        glueIcebergTableOperationsUnderTest.commitNewTable(metadata);

        // Verify the results
    }

    @Test
    public void testCommitNewTable_AWSGlueAsyncThrowsInvalidInputException()
    {
        // Setup
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(InvalidInputException.class);

        // Run the test
        glueIcebergTableOperationsUnderTest.commitNewTable(metadata);

        // Verify the results
    }

    @Test
    public void testCommitNewTable_AWSGlueAsyncThrowsEntityNotFoundException()
    {
        // Setup
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(EntityNotFoundException.class);

        // Run the test
        glueIcebergTableOperationsUnderTest.commitNewTable(metadata);

        // Verify the results
    }

    @Test
    public void testCommitNewTable_AWSGlueAsyncThrowsResourceNumberLimitExceededException()
    {
        // Setup
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest()))
                .thenThrow(ResourceNumberLimitExceededException.class);

        // Run the test
        glueIcebergTableOperationsUnderTest.commitNewTable(metadata);

        // Verify the results
    }

    @Test
    public void testCommitNewTable_AWSGlueAsyncThrowsInternalServiceException()
    {
        // Setup
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(InternalServiceException.class);

        // Run the test
        glueIcebergTableOperationsUnderTest.commitNewTable(metadata);

        // Verify the results
    }

    @Test
    public void testCommitNewTable_AWSGlueAsyncThrowsOperationTimeoutException()
    {
        // Setup
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(OperationTimeoutException.class);

        // Run the test
        glueIcebergTableOperationsUnderTest.commitNewTable(metadata);

        // Verify the results
    }

    @Test
    public void testCommitNewTable_AWSGlueAsyncThrowsGlueEncryptionException()
    {
        // Setup
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(GlueEncryptionException.class);

        // Run the test
        glueIcebergTableOperationsUnderTest.commitNewTable(metadata);

        // Verify the results
    }

    @Test
    public void testCommitToExistingTable()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("name");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        column.setName("name");
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenReturn(new UpdateTableResult());

        // Run the test
        glueIcebergTableOperationsUnderTest.commitToExistingTable(base, metadata);

        // Verify the results
    }

    @Test
    public void testCommitToExistingTable_AWSGlueAsyncGetTableThrowsEntityNotFoundException()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(EntityNotFoundException.class);
        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        glueIcebergTableOperationsUnderTest.commitToExistingTable(base, metadata);

        // Verify the results
    }

    @Test
    public void testCommitToExistingTable_AWSGlueAsyncGetTableThrowsInvalidInputException()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InvalidInputException.class);
        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        glueIcebergTableOperationsUnderTest.commitToExistingTable(base, metadata);

        // Verify the results
    }

    @Test
    public void testCommitToExistingTable_AWSGlueAsyncGetTableThrowsInternalServiceException()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InternalServiceException.class);
        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        glueIcebergTableOperationsUnderTest.commitToExistingTable(base, metadata);

        // Verify the results
    }

    @Test
    public void testCommitToExistingTable_AWSGlueAsyncGetTableThrowsOperationTimeoutException()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(OperationTimeoutException.class);
        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        glueIcebergTableOperationsUnderTest.commitToExistingTable(base, metadata);

        // Verify the results
    }

    @Test
    public void testCommitToExistingTable_AWSGlueAsyncGetTableThrowsGlueEncryptionException()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(GlueEncryptionException.class);
        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        glueIcebergTableOperationsUnderTest.commitToExistingTable(base, metadata);

        // Verify the results
    }

    @Test
    public void testCommitToExistingTable_AWSGlueAsyncUpdateTableThrowsEntityNotFoundException()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("name");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        column.setName("name");
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenThrow(EntityNotFoundException.class);

        // Run the test
        glueIcebergTableOperationsUnderTest.commitToExistingTable(base, metadata);

        // Verify the results
    }

    @Test
    public void testCommitToExistingTable_AWSGlueAsyncUpdateTableThrowsInvalidInputException()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("name");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        column.setName("name");
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenThrow(InvalidInputException.class);

        // Run the test
        glueIcebergTableOperationsUnderTest.commitToExistingTable(base, metadata);

        // Verify the results
    }

    @Test
    public void testCommitToExistingTable_AWSGlueAsyncUpdateTableThrowsInternalServiceException()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("name");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        column.setName("name");
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenThrow(InternalServiceException.class);

        // Run the test
        glueIcebergTableOperationsUnderTest.commitToExistingTable(base, metadata);

        // Verify the results
    }

    @Test
    public void testCommitToExistingTable_AWSGlueAsyncUpdateTableThrowsOperationTimeoutException()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("name");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        column.setName("name");
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenThrow(OperationTimeoutException.class);

        // Run the test
        glueIcebergTableOperationsUnderTest.commitToExistingTable(base, metadata);

        // Verify the results
    }

    @Test
    public void testCommitToExistingTable_AWSGlueAsyncUpdateTableThrowsConcurrentModificationException()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("name");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        column.setName("name");
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenThrow(ConcurrentModificationException.class);

        // Run the test
        glueIcebergTableOperationsUnderTest.commitToExistingTable(base, metadata);

        // Verify the results
    }

    @Test
    public void testCommitToExistingTable_AWSGlueAsyncUpdateTableThrowsResourceNumberLimitExceededException()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("name");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        column.setName("name");
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest()))
                .thenThrow(ResourceNumberLimitExceededException.class);

        // Run the test
        glueIcebergTableOperationsUnderTest.commitToExistingTable(base, metadata);

        // Verify the results
    }

    @Test
    public void testCommitToExistingTable_AWSGlueAsyncUpdateTableThrowsGlueEncryptionException()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("name");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        column.setName("name");
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenThrow(GlueEncryptionException.class);

        // Run the test
        glueIcebergTableOperationsUnderTest.commitToExistingTable(base, metadata);

        // Verify the results
    }
}
