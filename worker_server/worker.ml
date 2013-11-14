open Protocol

let mapperTable = Hashtbl.create 20
let reducerTable = Hashtbl.create 20
let mapperMutex = Mutex.create ()
let reducerMutex = Mutex.create ()
type init_type = MapInit | RedInit
type job_type = MapJob | RedJob

let send_response client response =
  let success = Connection.output client response in
    (if not success then
      (Connection.close client;
       print_endline "Connection lost before response could be sent.")
    else ());
    success

let rec handle_request (client : Connection.connection) : unit =
    let send_helper (id_option,str) initType = match initType with
        |MapInit -> send_response client (Mapper (id_option, str))
        |RedInit -> send_response client (Reducer (id_option, str)) in
    let init_helper source table mutex initType = 
        match Program.build source with
        |(Some id, str) -> 
                Mutex.lock mutex;
                Hashtbl.add table id str;
                Mutex.unlock mutex;

                if send_helper (Some id, str) initType then(
                    handle_request client)
                else ()
        |(None, err) -> if send_helper (None, err) initType then
                            handle_request client
                        else () in
    let map_helper id input =
        Mutex.lock mapperMutex;
        if (Hashtbl.mem mapperTable id) then (
            Mutex.unlock mapperMutex;
            match Program.run id input with
            |Some result -> if send_response client (MapResults (id, result)) then
                                handle_request client
                            else ()
            |None -> if send_response client (RuntimeError (id, "Runtime error in program")) then
                        handle_request client
                    else ()
        )
        else Mutex.unlock mapperMutex;
                if (send_response client (InvalidWorker id)) then
                    handle_request client
                else () in
    let reduce_helper id input = 
        Mutex.lock reducerMutex;
        if (Hashtbl.mem reducerTable id) then (
            Mutex.unlock reducerMutex;
            match Program.run id input with
            |Some result -> if send_response client (ReduceResults (id, result)) then
                                handle_request client
                            else ()
            |None -> if send_response client (RuntimeError (id, "Runtime error in program")) then
                        handle_request client
                    else ()
        )
        else Mutex.unlock reducerMutex;
                if (send_response client (InvalidWorker id)) then
                    handle_request client
                else () in
    match Connection.input client with
    |Some v ->
        begin
            match (v : worker_request) with
            | InitMapper source -> init_helper source mapperTable mapperMutex MapInit
            | InitReducer source -> init_helper source reducerTable reducerMutex RedInit
            | MapRequest (id, k, v) -> map_helper id (k,v)
            | ReduceRequest (id, k, v) -> reduce_helper id (k,v)
        end
    | None ->
      Connection.close client;
      print_endline "Connection lost while waiting for request."

