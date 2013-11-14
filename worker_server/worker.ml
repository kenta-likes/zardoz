open Protocol

let mapperTable = Hashtbl.create 20
let reducerTable = Hashtbl.create 20
let mapperMutex = Mutex.create ()
let reducerMutex = Mutex.create ()
type init_type = MapInit | RedInit

let send_response client response =
  let success = Connection.output client response in
    (if not success then
      (Connection.close client;
       print_endline "Connection lost before response could be sent.")
    else ());
    success

let rec handle_request (client : Connection.connection) : unit =
    let safe_add id str table mutex = 
        Mutex.lock mutex;
        Hashtbl.add table id str;
        Mutex.unlock mutex in
    let safe_remove id table mutex =
        Mutex.lock mutex;
        Hashtbl.remove table id;
        Mutex.unlock mutex in
    let send_helper (id_option,str) initType = match initType with
        |MapInit -> send_response client (Mapper (id_option, str))
        |RedInit -> send_response client (Reducer (id_option, str)) in
    let initHelper source table mutex initType = 
        match Program.build source with
        |(Some id, str) -> 
                safe_add id str table mutex;
                if send_helper (Some id, str) initType then
                    handle_request client
                else
                    safe_remove id table mutex
        |(None, err) -> if send_helper (None, err) initType then
                            handle_request client
                        else () in
    let safeCheck table mutex id =
        Mutex.lock mutex;
        Hashtbl.mem id in
    let workHelper i
    match Connection.input client with
    |Some v ->
    begin
        match (v : worker_request) with
        | InitMapper source -> initHelper source mapperTable mapperMutex MapInit
        | InitReducer source -> initHelper source reducerTable reducerMutex RedInit
        | MapRequest (id, k, v) -> 
          failwith "You won't go unrewarded."
        | ReduceRequest (id, k, v) -> 
          failwith "Really? In that case, just tell me what you need."
    end
        | None ->
          Connection.close client;
          print_endline "Connection lost while waiting for request."

