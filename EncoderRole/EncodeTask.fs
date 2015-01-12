#light

module UK.AC.Horizon.RunSpotRun.EncodeTask

open System
open System.Linq
open System.Threading
open System.Threading.Tasks

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob
open Microsoft.WindowsAzure.Storage.Queue
open Microsoft.WindowsAzure.MediaServices.Client

open RestSharp

open UK.AC.Horizon.RunSpotRun

let _name (ext : string) (s : string) =    
    sprintf "%s-%s" (s.Replace(".", "_")) ext
let input_name = _name "input"
let output_name = _name "output"
let job_name = _name "encoding-job"
let task_name = _name "encoding-task"

type EncodeException =
    inherit Exception
    new (msg)  = { inherit Exception(msg) }
    new (msg, (ex : Exception)) = { inherit Exception(msg, ex) }

let run (id : string) (config : Config.t) = 
    let storage = new CloudStorageAccount (config.blob_credentials, true)
    let queue_client = storage.CreateCloudQueueClient()
    let blob_client = storage.CreateCloudBlobClient()
    let context = new CloudMediaContext(config.media_credentials)
    let queue = queue_client.GetQueueReference(config.task_queue)
    let message_timeout = TimeSpan.FromMinutes(1.0)
    let task_timeout = TimeSpan.FromSeconds(30.0)
    let thread_pause_time = TimeSpan.FromMinutes(1.0)


    let copy_blob (from_blob : CloudBlockBlob) (to_blob : CloudBlockBlob) = 
        try
            let interval = TimeSpan.FromSeconds(10.0)
            to_blob.StartCopyFromBlob(from_blob) |> ignore
            let rec checkstatus () =
                match to_blob.Exists() with
                | true -> 
                    to_blob.FetchAttributes()
                    match to_blob.CopyState.Status with
                    | Blob.CopyStatus.Pending ->
                        Task.Delay(interval).Wait()
                        checkstatus ()
                    | Blob.CopyStatus.Aborted ->
                        Result.Error(new EncodeException("Blob copy aborted"))
                    | Blob.CopyStatus.Failed ->
                        Result.Error(new EncodeException("Blob copy failed"))
                    | Blob.CopyStatus.Invalid ->
                        Result.Error(new EncodeException("Blob copy invalid"))
                    | Blob.CopyStatus.Success -> Result.Ok()
                    | _ ->
                        Result.Error(new EncodeException("Blob copy <unknown status>"))
                | false ->
                    Task.Delay(interval).Wait()
                    checkstatus ()
            checkstatus ()
        with _ as e ->
            Result.Error(new EncodeException("An unexpected error occurred during a blob copy", e))


    let create_asset (video_name : string) = 
        try
            let upload_container = blob_client.GetContainerReference(config.raw_container)
            let raw_blob = upload_container.GetBlockBlobReference(video_name)
            match raw_blob.Exists() with
            | true ->
                let asset = context.Assets.Create(input_name video_name, AssetCreationOptions.None)
                let policy = context.AccessPolicies.Create("wp", TimeSpan.FromDays(1.0), AccessPermissions.Write)
                let locator = context.Locators.CreateLocator(LocatorType.Sas, asset, policy)
                let container = blob_client.GetContainerReference((new Uri(locator.Path)).Segments.[1])
                let file = asset.AssetFiles.Create(video_name)
                let to_blob = container.GetBlockBlobReference(video_name)
                to_blob.DeleteIfExists() |> ignore
                match copy_blob raw_blob to_blob with
                | Result.Ok _ -> 
                    locator.Delete()
                    policy.Delete()
                    Result.Ok(asset)
                | Result.Error e ->
                    Result.Error(e)
            | false -> Result.Error(new EncodeException("Blob does not exist"))
        with _ as e ->
            Result.Error(new EncodeException("An unexpected error occurred creating the encoding asset", e))


    let encode_video (video_name : string) (input_asset : IAsset) =
        try 
            let job = context.Jobs.Create(job_name video_name)
            let processor = context.MediaProcessors.GetLatestMediaProcessorByName(MediaProcessorNames.AzureMediaEncoder)
            let task = job.Tasks.AddNew(task_name video_name, processor, 
                        MediaEncoderTaskPresetStrings.H264Broadband720p,
                        TaskOptions.None)
            task.InputAssets.Add(input_asset)
            let output_asset = task.OutputAssets.AddNew(output_name video_name, AssetCreationOptions.None)
            job.StateChanged.Add(fun t -> sprintf "%s - Current encoding state = %A" id t.CurrentState |> Log.information)
            job.Submit()
            job.GetExecutionProgressTask(CancellationToken.None).Wait()
            input_asset.Delete()
            job.Delete()
            Result.Ok()
        with _ as e ->
            Result.Error(new EncodeException("An unexpected error during encoding", e))


    let extract_video (video_name : string) =
        try
            let asset = context.Assets 
                        |> Seq.choose 
                            (fun a -> 
                                match a.Name.Equals(output_name video_name) with 
                                | true -> Some(a) 
                                | _ -> None) |> Seq.head
            let choosefile (f : IAssetFile) =
                match f.Name.EndsWith("mp4", StringComparison.OrdinalIgnoreCase) with
                | true -> Some(f)
                | false -> None
            let out = asset.AssetFiles |> Seq.choose choosefile |> Seq.head
            let policy = context.AccessPolicies.Create("readPolicy", TimeSpan.FromDays(1.0), AccessPermissions.Read)
            let locator = context.Locators.CreateLocator(LocatorType.Sas, asset, policy)
            let fContainer = blob_client.GetContainerReference((new Uri(locator.Path)).Segments.[1])
            let from_blob = fContainer.GetBlockBlobReference(out.Name)
            let tContainer = blob_client.GetContainerReference("encoded")
            let to_blob = tContainer.GetBlockBlobReference(out.Name)
            to_blob.DeleteIfExists() |> ignore
            match copy_blob from_blob to_blob with
            | Result.Ok _ -> 
                locator.Delete()
                policy.Delete()
                asset.Delete()
                Result.Ok(to_blob)
            | Result.Error e ->
                Result.Error(e)
        with _ as e ->
            Result.Error(new EncodeException("An unexpected error extracting the encoded video", e))

    let rec loop () = 
        sprintf "%s - Checking for videos to encode" id |> Log.information 
        match queue.GetMessage(new Nullable<TimeSpan>(message_timeout)) with
        | null ->
            Task.Delay(thread_pause_time).Wait()
            loop ()
        | _ as message -> 
            let video_name = message.AsString
            sprintf "%s - Processing video with name '%s'" id video_name |> Log.information
            let task = new Task<Result.t<CloudBlockBlob, EncodeException>>
                        (fun () -> 
                        Result.result {
                            let! a = create_asset video_name
                            let! b = encode_video video_name a
                            let! c = extract_video video_name
                            return c
                        })
            task.Start()
            while not task.IsCompleted do
                queue.UpdateMessage(message, message_timeout, MessageUpdateFields.Visibility)
                task.Wait(task_timeout) |> ignore
            queue.DeleteMessage(message)
            match task.Result with
            | Result.Ok blob ->
                sprintf "%s - Encoded video with name '%s'" id blob.Name |> Log.information
                try
                    let rclient = new RestClient("https://www.runspotrun.co.uk/api/v1")
                    let req = new RestRequest("video/guid/{id}", Method.PUT)
                    let vid = video_name.Substring(video_name.IndexOf('_') + 1)
                    req.AddQueryParameter("format", "json")
                            .AddQueryParameter("username", config.server_username)
                            .AddQueryParameter("api_key", config.server_api_key)
                            .AddUrlSegment("id", vid) |> ignore
                    req.RequestFormat <- RestSharp.DataFormat.Json
                    req.AddBody(Map.ofList [("guid", vid); ("url", blob.Uri.ToString())]) |> ignore
                    rclient.Execute(req) |> ignore
                with _ as e ->
                    sprintf "%s - An error occurred updating the server: %s (%A)" id e.Message e.StackTrace |> Log.error
            | Result.Error e ->
                sprintf "%s - An error occurred: %s (%A)" id e.Message e.StackTrace |> Log.error
            loop ()
        ()
    loop ()