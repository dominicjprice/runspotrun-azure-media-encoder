#light

module UK.AC.Horizon.RunSpotRun.Result

type t<'ok, 'err> = 
    | Ok of 'ok
    | Error of 'err

type ResultBuilder =
    new : unit -> ResultBuilder
    member Bind : t<'c, 'd> * ('c -> t<'e, 'd>) -> t<'e, 'd>
    member Return : 'a -> t<'a, 'b>

val result : ResultBuilder