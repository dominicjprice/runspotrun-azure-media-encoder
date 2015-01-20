#light

module UK.AC.Horizon.RunSpotRun.Result

type t<'ok, 'err> = 
    | Ok of 'ok
    | Error of 'err

type ResultBuilder() =

    member this.Bind(x, f) = 
        match x with
        | Ok ok -> f ok
        | Error e -> Error e

    member this.Return(x) = 
        Ok x

let result = new ResultBuilder()
