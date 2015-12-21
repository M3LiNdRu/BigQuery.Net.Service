using Custom.BigQuery.Service;
using Google.Apis.Bigquery.v2;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CustomBigQuery.Test
{
    [TestClass]
    public class UnitTestQuery
    {
        private AuthService auth = new AuthService();

        [TestMethod]
        public void TestMethodQueryJSON() {

            BigqueryService bq = auth.getServiceAuthP12();
            QueryService q = new QueryService(bq, "api-project-109606543851");
            JObject json = q.GetData("SELECT station_number, wban_number FROM [publicdata:samples.gsod] LIMIT 10");
            Debug.WriteLine(json);
            Assert.IsNotNull(bq);

        }

        [TestMethod]
        public void TestMethodQueryObject()
        {

            BigqueryService bq = auth.getServiceAuthP12();
            QueryService q = new QueryService(bq, "api-project-109606543851");
            List<GsodModel> obj = q.GetData<GsodModel>("SELECT station_number, wban_number FROM [publicdata:samples.gsod] LIMIT 10");
            Debug.WriteLine(obj);
            Assert.IsNotNull(obj);

        }
    }
}
