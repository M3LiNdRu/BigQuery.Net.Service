using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Custom.BigQuery.Service
{
    public class QueryService
    {
        private BigqueryService bq;
        private string projectId;
        private BigQueryDataSerializer deserializer;

        public QueryService(BigqueryService service, string projectId)
        {
            this.bq = service;
            this.projectId = projectId;
            this.deserializer = new BigQueryDataSerializer();
        }

        //TODO: Implement pagination
        public JObject GetData(string query)
        {
            JObject list = new JObject();
            JobsResource j = bq.Jobs;
            QueryRequest qr = new QueryRequest();
            qr.Query = query;

            QueryResponse response = j.Query(qr, projectId).Execute();
            if (response.Rows != null)
            {
                int cnt = 0;
                foreach (TableRow row in response.Rows)
                {
                    int cnt2 = 0;
                    JObject element = new JObject();
                    foreach (TableCell field in row.F)
                    {
                        element.Add(new JProperty("Camp " + cnt2, field.V));
                        ++cnt2;
                    }
                    list.Add("Fila " + cnt, element);
                    ++cnt;
                }

            }
            else list.Add("null");
            return list;
        }

        //TODO: Implement pagination
        public List<T> GetData<T>(string query) where T : new()
        {
            List<T> rows;
            JobsResource j = bq.Jobs;
            QueryRequest qr = new QueryRequest();
            qr.Query = query;

            QueryResponse response = j.Query(qr, projectId).Execute();
            if (response.JobComplete ?? false)
            {
                rows = response.Rows.Select(row => deserializer.Deserialize<T>(row, response.Schema)).ToList();
                return rows;
            }
            return null;
        }
    }
}
