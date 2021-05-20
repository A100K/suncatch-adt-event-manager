// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using Azure;
using Azure.Core.Pipeline;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace suncatch_adt_event_manager
{
    public static class ProcessDTRoutedData
    {
        private static readonly HttpClient httpClient = new HttpClient();
        private static string adtServiceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");

        [FunctionName("ProcessWindUpdate")]
        public static async Task Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            DigitalTwinsClient client;
            // Authenticate on ADT APIs
            try
            {
                var credentials = new DefaultAzureCredential();
                client = new DigitalTwinsClient(new Uri(adtServiceUrl), credentials, new DigitalTwinsClientOptions { Transport = new HttpClientTransport(httpClient) });
            }
            catch(Exception e)
            {
                log.LogError($"ADT service client connection failed. {e}");
                return;
            }

            if(client != null)
            {
                if(eventGridEvent != null && eventGridEvent.Data != null)
                {
                    string twinId = eventGridEvent.Subject.ToString();
                    JObject message = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());

                    log.LogInformation($"Reading event from {twinId}: {eventGridEvent.EventType}: {message["data"]}");

                    // Read properties which values have been changed in each operation
                    foreach(var operation in message["data"]["patch"])
                    {
                        string opValue = (string)operation["op"];
                        if(opValue.Equals("replace"))
                        {
                            string propertyPath = ((string)operation["path"]);

                            if(propertyPath.Equals("/CurrentWind"))
                            {
                                float currentWind = operation["value"].Value<float>();
                                AsyncPageable<IncomingRelationship> windAlertRelashion = client.GetIncomingRelationshipsAsync(twinId);
                                await foreach(IncomingRelationship windAlert in windAlertRelashion)
                                {
                                    if(windAlert.RelationshipName == "windAlert")
                                    {
                                        try
                                        {
                                            if(currentWind > 20)
                                            {
                                                await AdtUtilities.UpdateTwinPropertyAsync(client, windAlert.SourceId, "/Mode", "Safety", log);
                                                log.LogInformation($"Wind alert triggered on {windAlert.SourceId}, mode changed to Safety");
                                            }
                                            else
                                            {
                                                await AdtUtilities.UpdateTwinPropertyAsync(client, windAlert.SourceId, "/Mode", "Auto", log);
                                                log.LogInformation($"Wind alert triggered on {windAlert.SourceId}, mode changed to Safety");
                                            }
                                        }
                                        catch(Exception e)
                                        {
                                            log.LogError($"failed to update {windAlert.SourceId}. Error:{e}");
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}