using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2;
using Google.Apis.Services;
using Google.Apis.Util.Store;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Custom.BigQuery.Service
{
    public class AuthService
    {
        private static string projectId = "api-project-XXXXXXXXXXXX";
        private static byte[] jsonSecrets = Custom.BigQuery.Service.Properties.Resources.client_secrets;
        private static string serviceAccountEmail = "XXXXXXXXXXXX-asdfasdgfsdafasdfewrf4@developer.gserviceaccount.com";
        private static string P12password = "XXXXXXX";
        private static byte[] P12Key = Properties.Resources.API_Project_XXXXXXXXXXXX;
        private static byte[] JSONKey = Properties.Resources.API_Project_XXXXXXXXXXXX;


        private static string APPLICATION_NAME = "Google-BigQuery-CustomService/v0.1";

        public BigqueryService getServiceAuthP12()
        {

            try
            {

                X509Certificate2 certificate = new X509Certificate2(P12Key, P12password, X509KeyStorageFlags.Exportable);
                ServiceAccountCredential credential;
                credential = new ServiceAccountCredential(new ServiceAccountCredential.
                Initializer(serviceAccountEmail)
                {
                    Scopes = new[] { BigqueryService.Scope.Bigquery, BigqueryService.Scope.CloudPlatform }
                }.FromCertificate(certificate));

                var service = new BigqueryService(new BaseClientService.Initializer()
                {
                    HttpClientInitializer = credential,
                    ApplicationName = APPLICATION_NAME
                });

                return service;
                
            }
            catch (Exception e)
            {

                String a = e.InnerException.Data.ToString();
                return null;

            }
        }

        //TODO: Not Implemented
        public BigqueryService getServiceAuthJSON()
        {
            try {

                return null;
            }
            catch (Exception e) { return null; }
        }

        public BigqueryService getAuth()
        {
            try
            {
                UserCredential credential;
                using (var stream = new MemoryStream(jsonSecrets))
                {
                    GoogleWebAuthorizationBroker.Folder = "Bigquery.Auth.Store";
                    credential = GoogleWebAuthorizationBroker.AuthorizeAsync(
                        GoogleClientSecrets.Load(stream).Secrets,
                        new[] { BigqueryService.Scope.Bigquery, BigqueryService.Scope.CloudPlatform },
                        "user",
                        CancellationToken.None,
                        new FileDataStore("Bigquery.Auth.Store")
                    ).Result;
                }
                // Create the service.
                var service = new BigqueryService(new BaseClientService.Initializer()
                {
                    HttpClientInitializer = credential,
                    ApplicationName = APPLICATION_NAME
                });

                return service;
            }
            catch (Exception e)
            {
                String a = e.InnerException.Data.ToString();
                return null;
            }
        }
    }
}
