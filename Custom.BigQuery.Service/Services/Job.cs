using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Custom.BigQuery.Service
{
    public class JobService
    {
        private BigqueryService bq;
        private string projectId;
        private BigQueryDataSerializer serializer;


        public JobService(BigqueryService service, string projectId)
        {
            this.bq = service;
            this.projectId = projectId;
            this.serializer = new BigQueryDataSerializer();
        }

        public void InsertAll<T>(List<T> data, string datasetId, string tableId) {

            //Map original object to destiny
            List<TableDataInsertAllRequest.RowsData> rows = data.Select(x => serializer.Serialize<T>(x)).ToList();
            
            try
            {
                TabledataResource t = this.bq.Tabledata;
                TableDataInsertAllRequest req = new TableDataInsertAllRequest()
                {
                    Kind = "bigquery#tableDataInsertAllRequest",
                    Rows = rows 
                };
                TableDataInsertAllResponse response = t.InsertAll(req, projectId, datasetId, tableId).Execute();
                if (response.InsertErrors != null)
                {

                }
               
            }
            catch (Exception e)
            {
               
            }

        }

        public void InsertPOST()
        {

        }
    }
}
