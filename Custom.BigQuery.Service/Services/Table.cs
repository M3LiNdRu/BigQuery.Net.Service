using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Custom.BigQuery.Service
{
    public class TableService
    {
        private BigqueryService bq;
        private string projectId;
        private BigQueryDataSerializer serializer;


        public TableService(BigqueryService service, string projectId)
        {
            this.bq = service;
            this.projectId = projectId;
            this.serializer = new BigQueryDataSerializer();
        }

        public bool CreateTable(Type type, string datasetId)
        {
            Table body = createBody(type, datasetId);
            TablesResource t = new TablesResource(this.bq);
            Table response = t.Insert(body, this.projectId, datasetId).Execute();

            return body.Equals(response);
        }

        /// <summary>
        /// Replace old table for new table with empty rows
        /// </summary>
        /// <param name="s"></param>
        /// <param name="datasetId"></param>
        /// <param name="tableId"></param>
        public bool ReplaceTable(String datasetId, String tableId)
        {
            TablesResource t = new TablesResource(this.bq);
            Table body = t.Get(projectId, datasetId, tableId).Execute();
            if (this.DeleteTable(datasetId, tableId) == "")
            {
                Table newTable = new Table()
                {
                    Schema = body.Schema,
                    TableReference = new TableReference()
                    {
                        DatasetId = datasetId,
                        ProjectId = projectId,
                        TableId = body.TableReference.TableId
                    }
                };
                Table aux = t.Insert(newTable, projectId, datasetId).Execute();
                return body.Equals(aux);
            }

            return false;
            
        }

        public string DeleteTable(String datasetId, String tableId)
        {
            TablesResource t = new TablesResource(this.bq);
            return t.Delete(projectId, datasetId, tableId).Execute();
        }

        private Table createBody(Type t, string datasetId)
        {
            Table body = new Table()
            {
                Description = t.FullName,
                FriendlyName = t.Name,
                Schema = this.serializer.GetSchema(t),
                TableReference = new TableReference() { DatasetId = datasetId, ProjectId = this.projectId, TableId = t.Name }
            };

            return body;
        }
    }
}
