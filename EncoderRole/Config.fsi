#light

module UK.AC.Horizon.RunSpotRun.Config

val inline value<'T> : string -> 'T

type t = {
    task_queue : string
    raw_container : string
    encoded_container : string
    blob_credentials : Microsoft.WindowsAzure.Storage.Auth.StorageCredentials
    media_credentials : Microsoft.WindowsAzure.MediaServices.Client.MediaServicesCredentials
    server_username : string
    server_api_key : string
}
