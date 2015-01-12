namespace UK.AC.Horizon.RunSpotRun.EncoderRole

open System
open System.Collections.Generic
open System.Diagnostics
open System.Linq
open System.Net
open System.Threading
open Microsoft.WindowsAzure
open Microsoft.WindowsAzure.Diagnostics
open Microsoft.WindowsAzure.MediaServices.Client
open Microsoft.WindowsAzure.ServiceRuntime
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Auth
open Microsoft.WindowsAzure.Storage.Blob
open Microsoft.WindowsAzure.Storage.Queue

open UK.AC.Horizon.RunSpotRun

type EncoderRole() =
    inherit RoleEntryPoint() 

    let init_storage (config : Config.t) = 
        let account = new CloudStorageAccount(config.blob_credentials, true)
        let queue_client = account.CreateCloudQueueClient()
        let blob_client = account.CreateCloudBlobClient()
        queue_client.GetQueueReference(config.task_queue).CreateIfNotExists() |> ignore
        blob_client.GetContainerReference(config.raw_container).CreateIfNotExists() |> ignore
        blob_client.GetContainerReference(config.encoded_container).CreateIfNotExists() |> ignore

    override wr.Run() =

        let config = {
            Config.t.task_queue = Config.value "task_queue"
            Config.t.raw_container = Config.value "raw_container"
            Config.t.encoded_container = Config.value "encoded_container"
            Config.t.blob_credentials = new StorageCredentials(
                                            Config.value<string> "blob_account_name",
                                            Config.value<string> "blob_account_key")
            Config.t.media_credentials = new MediaServicesCredentials(
                                            Config.value<string> "media_account_name", 
                                            Config.value<string> "media_account_key")
            Config.t.server_username = Config.value "server_username"
            Config.t.server_api_key = Config.value "server_api_key"
        }

        init_storage config

        seq { for i in [0..9] do yield sprintf "thread-%i" i } 
            |> Seq.map (fun id -> async { EncodeTask.run id config }) 
            |> Async.Parallel
            |> Async.RunSynchronously 
            |> ignore

        while true do
            Thread.Sleep(1000)

    override wr.OnStart() =         
        ServicePointManager.DefaultConnectionLimit <- 12
        base.OnStart()
